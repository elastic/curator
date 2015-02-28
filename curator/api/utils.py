import elasticsearch
import time
import re
from datetime import timedelta, datetime, date

import logging
logger = logging.getLogger(__name__)

def get_alias(client, alias):
    """
    Return information about the specified alias.

    :arg client: The Elasticsearch client connection
    :arg alias: Alias name to operate on.
    :rtype: list of strings
    """
    if client.indices.exists_alias(alias):
        return client.indices.get_alias(name=alias).keys()
    else:
        logger.error('Unable to find alias {0}.'.format(alias))
        return False

def get_indices(client):
    try:
        indices = client.indices.get_settings(
            index='*', params={'expand_wildcards': 'open,closed'}).keys()
        logger.debug("All indices: {0}".format(indices))
        return indices
    except Exception as e:
        logger.error("Failed to get indices. Exception: {0}".format(e.message))
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
        if len(indices) > 1:
            return ','.join(sorted(indices))
        elif len(indices) == 1:
            return indices[0]
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
    elif type(value) is type(str()):
        if len(value.split(',')) > 1: # It's a csv string.
            return True
        else: # There's only one value here, so it's not a csv string
            return False
    else:
        logger.error("Value is not a list or a string")
        return None

def prune_kibana(indices):
    """Remove any index named .kibana, kibana-int, or .marvel-kibana

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

### Index state
def index_closed(client, index_name):
    """
    Return `True` if the indicated index is closed.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :rtype: bool
    """
    index_metadata = client.cluster.state(
        index=index_name,
        metric='metadata',
    )
    return index_metadata['metadata']['indices'][index_name]['state'] == 'close'

def prune_closed(client, indices):
    """
    Return list of indices that are not closed.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: list
    """
    indices = ensure_list(indices)
    retval = []
    for idx in indices:
        if not index_closed(client, idx):
            retval.append(idx)
        else:
            logger.info('Skipping index {0}: Already closed.'.format(idx))
    return sorted(retval)

def get_segmentcount(client, index_name):
    """
    Return a tuple of ``(shardcount, segmentcount)`` from the provided
    ``index_name``.

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

## Client state
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
    my_node_id = client.nodes.info('_local')['nodes'].keys()[0]
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
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as e:
        logger.error("Repository {0} not found.  Error: {1}".format(repository, e.message))
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
    if not snapshot:
        logger.error('Missing required snapshot parameter')
        return False
    try:
        return client.snapshot.get(repository=repository, snapshot=snapshot)
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as e:
        logger.error("Snapshot: {0} or repository: {1} not found.  Exception: {2}".format(snapshot, repository, e.message))
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
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as e:
        logger.error("Unable to find all snapshots in repository: {0}  Exception: {1}".format(repository, e.message))
        return False

def create_snapshot_body(indices, ignore_unavailable=False,
                         include_global_state=True, partial=False):
    """
    Create the request body for creating a snapshot from the provided
    arguments.

    :arg indices: A single index, or list of indices to snapshot.
    :arg ignore_unavailable: Boolean. Ignore unavailable shards/indices.
        Default is `False`
    :arg include_global_state: Boolean. Store cluster global state with snapshot.
        Default is `True`
    :arg partial: Boolean. Do not fail if primary shard is unavailable. Default
        is `False`
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