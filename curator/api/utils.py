from datetime import timedelta, datetime, date
import elasticsearch
import time
import re
import sys
import logging
import json
import click
logger = logging.getLogger(__name__)

def get_alias(client, alias):
    """
    Return information about the specified alias.

    :arg client: The Elasticsearch client connection
    :arg alias: Alias name to operate on.
    :rtype: list of strings
    """
    if client.indices.exists_alias(name=alias):
        return client.indices.get_alias(name=alias).keys()
    else:
        logger.error('Unable to find alias {0}.'.format(alias))
        return False

def get_indices(client):
    try:
        indices = list(client.indices.get_settings(
            index='_all', params={'expand_wildcards': 'open,closed'}))
        logger.debug("All indices: {0}".format(indices))
        return indices
    except Exception:
        logger.error("Failed to get indices.")
        return False

def ensure_list(indices):
    """
    Return a list, even if indices is a single value

    :arg indices: A list of indices to act upon
    :rtype: list
    """
    if type(indices) is not type(list()):   # in case of a single value passed
        indices = [indices]
    return indices

def to_csv(indices):
    """
    Return a csv string from a list of indices, or a single value if only one
    value is present

    :arg indices: A list of indices to act on, or a single value, which could be
        in the format of a csv string already.
    :rtype: str
    """
    indices = ensure_list(indices) # in case of a single value passed
    if indices:
        return ','.join(sorted(indices))
    else:
        return None

def check_csv(value):
    """
    Some of the curator methods should not operate against multiple indices at
    once.  This method can be used to check if a list or csv has been sent.

    :arg value: The value to test, if list or csv string
    :rtype: bool
    """
    if type(value) is type(list()):
        return True
    string = False
    # Python3 hack because it doesn't recognize unicode as a type anymore
    if sys.version_info < (3, 0):
        if type(value) is type(unicode()):
            value = str(value)
    if type(value) is type(str()):
        if len(value.split(',')) > 1: # It's a csv string.
            return True
        else: # There's only one value here, so it's not a csv string
            return False
    else:
        logger.error("Passed value: {0} is not a list or a string but is of type {1}".format(value, type(value)))
        sys.exit(1)

def index_closed(client, index_name):
    """
    Return `True` if the indicated index is closed.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :rtype: bool
    """
    try:
        index_metadata = client.cluster.state(
            index=index_name,
            metric='metadata',
        )
        return index_metadata['metadata']['indices'][index_name]['state'] == 'close'
    except elasticsearch.TransportError as e:
        if e.status_code == 401:
            # This workaround using the _cat API is for AWS Elasticsearch, since it does
            # not allow users to poll the cluster state.  It is slower than using the
            # cluster state, especially when iterating over large numbers of indices.
            try:
                if get_version(client) >= (1, 5, 0):
                    indices = client.cat.indices(index=index_name, format='json', h='status')
                    # This workaround is also for AWS, as the _cat API still returns text/plain
                    # even with ``format='json'``.
                    if not isinstance(indices, list):  # content-type: text/plain
                        indices = json.loads(indices)
                    (index_info,) = indices
                    return index_info['status'] == 'close'
                else:
                    logger.error('Unable to determine whether index {0} is open or closed'.format(index_name))
                    raise
            except Exception:
                logger.error('Unable to determine whether index {0} is open or closed'.format(index_name))
                raise
        else:
            logger.debug('Error {0}'.format(e))
    else:
        logger.error('Unable to determine whether index {0} is open or closed'.format(index_name))
        raise



def get_segmentcount(client, index_name):
    """
    Return a tuple of `(shardcount, segmentcount)` from the provided
    `index_name`.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :rtype: tuple
    """
    shards = client.indices.segments(index=index_name)['indices'][index_name]['shards']
    segmentcount = 0
    totalshards = 0 # We will increment this manually to capture all replicas...
    for shardnum in shards:
        for shard in range(0,len(shards[shardnum])):
            segmentcount += shards[shardnum][shard]['num_search_segments']
            totalshards += 1
    return totalshards, segmentcount

def chunk_index_list(indices):
    """
    This utility chunks very large index lists into 3KB chunks
    It measures the size as a csv string, then converts back into a list
    for the return value.

    :arg indices: A list of indices to act on.
    """
    chunks = []
    chunk = ""
    for index in indices:
        if len(chunk) < 3072:
            if not chunk:
                chunk = index
            else:
                chunk += "," + index
        else:
            chunks.append(chunk.split(','))
            chunk = index
    chunks.append(chunk.split(','))
    return chunks

def optimized(client, index_name, max_num_segments=None):
    """
    Check if an index is optimized.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg max_num_segments: Merge to this number of segments per shard.
    :rtype: bool
    """
    if not max_num_segments:
        logger.error("Mising value for max_num_segments.")
        raise ValueError
    if check_csv(index_name):
        logger.error("Must specify only a single index as an argument.")
        raise ValueError
    if index_closed(client, index_name): # Don't try to optimize a closed index
        logger.debug('Skipping index {0}: Already closed.'.format(index_name))
        return True
    shards, segmentcount = get_segmentcount(client, index_name)
    logger.debug('Index {0} has {1} shards and {2} segments total.'.format(index_name, shards, segmentcount))
    if segmentcount > (shards * max_num_segments):
        logger.debug('Flagging index {0} for optimization.'.format(index_name))
        return False
    else:
        logger.debug('Skipping index {0}: Already optimized.'.format(index_name))
        return True

def get_version(client):
    """
    Return the ES version number as a tuple.
    Omits trailing tags like -dev, or Beta

    :arg client: The Elasticsearch client connection
    :rtype: tuple
    """
    version = client.info()['version']['number']
    version = version.split('-')[0]
    if len(version.split('.')) > 3:
        version = version.split('.')[:-1]
    else:
       version = version.split('.')
    return tuple(map(int, version))

def is_master_node(client):
    """
    Return `True` if the connected client node is the elected master node in
    the Elasticsearch cluster, otherwise return `False`.

    :arg client: The Elasticsearch client connection
    :rtype: bool
    """
    my_node_id = list(client.nodes.info('_local')['nodes'])[0]
    master_node_id = client.cluster.state(metric='master_node')['master_node']
    return my_node_id == master_node_id

def get_repository(client, repository=''):
    """
    Return configuration information for the indicated repository.

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: dict
    """
    try:
        return client.snapshot.get_repository(repository=repository)
    except (elasticsearch.TransportError, elasticsearch.NotFoundError):
        logger.error("Repository {0} not found.".format(repository))
        return False

def get_snapshot(client, repository='', snapshot=''):
    """
    Return information about a snapshot (or a comma-separated list of snapshots)
    If no snapshot specified, it will return all snapshots.  If none exist, an
    empty dictionary will be returned.

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name, or a comma-separated list of snapshots
    :rtype: dict
    """
    if not repository:
        logger.error('Missing required repository parameter')
        return False
    try:
        return client.snapshot.get(repository=repository, snapshot=snapshot)
    except (elasticsearch.TransportError, elasticsearch.NotFoundError):
        logger.error("Snapshot: {0} or repository: {1} not found.".format(snapshot, repository))
        return False

def get_snapshots(client, repository=None):
    """
    Get ``_all`` snapshots from repository and return a list.

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: list of strings
    """
    if not repository:
        logger.error('Missing required repository parameter')
        return False
    try:
        allsnaps = client.snapshot.get(repository=repository, snapshot="_all")['snapshots']
        return [snap['snapshot'] for snap in allsnaps if 'snapshot' in snap.keys()]
    except (elasticsearch.TransportError, elasticsearch.NotFoundError):
        logger.error("Unable to find all snapshots in repository: {0}".format(repository))
        return False

def create_snapshot_body(indices, ignore_unavailable=False,
                         include_global_state=True, partial=False):
    """
    Create the request body for creating a snapshot from the provided
    arguments.

    :arg indices: A single index, or list of indices to snapshot.
    :arg ignore_unavailable: Boolean. Ignore unavailable shards/indices.
        (default: `False`)
    :arg include_global_state: Boolean. Store cluster global state with snapshot.
        (default: `True`)
    :arg partial: Boolean. Do not fail if primary shard is unavailable.
        (default: `False`)
    :rtype: dict
    """
    if not indices:
        logger.error('Missing required repository parameter')
        return False
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if indices == '_all':
        body["indices"] = indices
    else:
        body["indices"] = to_csv(indices)
    return body

def delete_callback(ctx, param, value):
    if not value:
        ctx.abort()

def create_repo_body(repo_type=None,
                     compress=True, chunk_size=None,
                     max_restore_bytes_per_sec=None,
                     max_snapshot_bytes_per_sec=None,
                     location=None,
                     bucket=None, region=None, base_path=None, access_key=None,
                     secret_key=None, **kwargs):
    """
    Build the 'body' portion for use in creating a repository.

    :arg repo_type: The type of repository (presently only `fs` and `s3`)
    :arg compress: Turn on compression of the snapshot files. Compression is
        applied only to metadata files (index mapping and settings). Data files
        are not compressed. (Default: `True`)
    :arg chunk_size: The chunk size can be specified in bytes or by using size
        value notation, i.e. 1g, 10m, 5k. Defaults to `null` (unlimited chunk
        size).
    :arg max_restore_bytes_per_sec: Throttles per node restore rate. Defaults
        to ``20mb`` per second.
    :arg max_snapshot_bytes_per_sec: Throttles per node snapshot rate. Defaults
        to ``20mb`` per second.
    :arg location: Location of the snapshots. Required.
    :arg bucket: `S3 only.` The name of the bucket to be used for snapshots.
        Required.
    :arg region: `S3 only.` The region where bucket is located. Defaults to
        `US Standard`
    :arg base_path: `S3 only.` Specifies the path within bucket to repository
        data. Defaults to value of ``repositories.s3.base_path`` or to root
        directory if not set.
    :arg access_key: `S3 only.` The access key to use for authentication.
        Defaults to value of ``cloud.aws.access_key``.
    :arg secret_key: `S3 only.` The secret key to use for authentication.
        Defaults to value of ``cloud.aws.secret_key``.

    :returns: A dictionary suitable for creating a repository from the provided
        arguments.
    :rtype: dict
    """
    # This shouldn't happen, but just in case...
    if not repo_type:
        click.echo(click.style('Missing required parameter --repo_type', fg='red', bold=True))
        sys.exit(1)

    argdict = locals()
    body = {}
    body['type'] = argdict['repo_type']
    body['settings'] = {}
    settings = []
    maybes   = [
                'compress', 'chunk_size',
                'max_restore_bytes_per_sec', 'max_snapshot_bytes_per_sec'
               ]
    s3       = ['bucket', 'region', 'base_path', 'access_key', 'secret_key']

    settings += [i for i in maybes if argdict[i]]
    # Type 'fs'
    if argdict['repo_type'] == 'fs':
        settings.append('location')
    # Type 's3'
    if argdict['repo_type'] == 's3':
        settings += [i for i in s3 if argdict[i]]
    for k in settings:
        body['settings'][k] = argdict[k]
    return body

def create_repository(client, **kwargs):
    """
    Create repository with repository and body settings

    :arg client:  The Elasticsearch client connection

    :arg repo_type: The type of repository (presently only `fs` and `s3`)
    :arg compress: Turn on compression of the snapshot files. Compression is
        applied only to metadata files (index mapping and settings). Data files
        are not compressed. (Default: `True`)
    :arg chunk_size: The chunk size can be specified in bytes or by using size
        value notation, i.e. 1g, 10m, 5k. Defaults to `null` (unlimited chunk
        size).
    :arg max_restore_bytes_per_sec: Throttles per node restore rate. Defaults
        to ``20mb`` per second.
    :arg max_snapshot_bytes_per_sec: Throttles per node snapshot rate. Defaults
        to ``20mb`` per second.
    :arg location: Location of the snapshots. Required.
    :arg bucket: `S3 only.` The name of the bucket to be used for snapshots.
        Required.
    :arg region: `S3 only.` The region where bucket is located. Defaults to
        `US Standard`
    :arg base_path: `S3 only.` Specifies the path within bucket to repository
        data. Defaults to value of ``repositories.s3.base_path`` or to root
        directory if not set.
    :arg access_key: `S3 only.` The access key to use for authentication.
        Defaults to value of ``cloud.aws.access_key``.
    :arg secret_key: `S3 only.` The secret key to use for authentication.
        Defaults to value of ``cloud.aws.secret_key``.

    :returns: A boolean value indicating success or failure.
    :rtype: boolean
    """
    if not 'repository' in kwargs:
        click.echo(click.style('Missing required parameter --repository', fg='red', bold=True))
        sys.exit(1)
    else:
        repository = kwargs['repository']

    try:
        body = create_repo_body(**kwargs)
        logger.info("Checking if repository {0} already exists...".format(repository))
        result = get_repository(client, repository=repository)
        logger.debug("Result = {0}".format(result))
        if not result:
            logger.info("Repository {0} not in Elasticsearch. Continuing...".format(repository))
            client.snapshot.create_repository(repository=repository, body=body)
        elif result is not None and repository not in result:
            logger.info("Repository {0} not in Elasticsearch. Continuing...".format(repository))
            client.snapshot.create_repository(repository=repository, body=body)
        else:
            logger.error("Unable to create repository {0}.  A repository with that name already exists.".format(repository))
            sys.exit(1)
    except elasticsearch.TransportError as e:
        logger.error("Unable to create repository {0}.  Response Code: {1}.  Error: {2}. Check curator and elasticsearch logs for more information.".format(repository, e.status_code, e.error))
        return False
    logger.info("Repository {0} creation initiated...".format(repository))
    return True

def verify_repository(client, repository=None):
    """
    Verify the existence of a repository

    Args:
        client:  The Elasticsearch client connection

    Kwargs:
        repository: The Elasticsearch snapshot repository to use

    Returns:
        A boolean value indicating success or failure.
    """
    if not repository:
        click.echo(click.style('Missing required parameter --repository', fg='red', bold=True))
        sys.exit(1)
    test_result = get_repository(client, repository)
    if repository in test_result:
        logger.info("Repository {0} creation validated.".format(repository))
        return True
    else:
        logger.error("Repository {0} failed validation...".format(repository))
        return False

def prune_kibana(indices):
    """Remove any index named `.kibana`, `kibana-int`, or `.marvel-kibana`

    :arg indices: A list of indices to act upon.
    :rtype: list
    """
    indices = ensure_list(indices)
    if '.marvel-kibana' in indices:
        indices.remove('.marvel-kibana')
    if 'kibana-int' in indices:
        indices.remove('kibana-int')
    if '.kibana' in indices:
        indices.remove('.kibana')
    return indices

def prune_closed(client, indices):
    """
    Return list of indices that are not closed.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: list
    """
    return prune_open_or_closed(client, indices, append_closed=False)

def prune_opened(client, indices):
    """
    Return list of indices that are not open.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: list
    """
    return prune_open_or_closed(client, indices, append_closed=True)

def prune_open_or_closed(client, indices, append_closed):
    indices = ensure_list(indices)
    retval = []
    status = 'Closed' if append_closed else 'Opened'
    for idx in list(indices):
        if index_closed(client, idx) is append_closed:
            retval.append(idx)
            logger.debug('Including index {0}: {1}.'.format(idx, status))
        else:
            logger.debug('Skipping index {0}: {1}.'.format(idx, status))
    return sorted(retval)

def prune_allocated(client, indices, key, value, allocation_type):
    """
    Return list of indices that do not have the routing allocation rule of
    `key=value`

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg key: The allocation attribute to check for
    :arg value: The value to check for
    :arg allocation_type: Type of allocation to apply
    :rtype: list
    """
    indices = ensure_list(indices)
    retval = []
    for idx in indices:
        settings = client.indices.get_settings(
            index=idx,
        )
        try:
            has_routing = settings[idx]['settings']['index']['routing']['allocation'][allocation_type][key] == value
        except KeyError:
            has_routing = False
        if has_routing:
            logger.debug('Skipping index {0}: Allocation rule {1} is already applied for type {2}.'.format(idx, key + "=" + value, allocation_type))
        else:
            logger.info('Flagging index {0} to have allocation rule {1} applied for type {2}.'.format(idx, key + "=" + value, allocation_type))
            retval.append(idx)
    return sorted(retval)
