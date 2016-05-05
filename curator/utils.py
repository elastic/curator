from datetime import timedelta, datetime, date
import elasticsearch
import copy
import time
import re
import sys
import logging
import yaml
from .exceptions import *
from .settings import DATE_REGEX, VERSION_MAX, VERSION_MIN, ACTION_DEFAULTS
from ._version import __version__
logger = logging.getLogger(__name__)

def read_file(myfile):
    """
    Read a file and return the resulting data.

    :arg myfile: A file to read.
    :rtype: str
    """
    try:
        with open(myfile, 'r') as f:
            data = f.read()
        return data
    except IOError as e:
        raise FailedExecution(
            'Unable to read file {0}'.format(myfile)
        )

def get_yaml(path):
    """
    Read the file identified by `path` and import its YAML contents.

    :arg path: The path to a YAML configuration file.
    :rtype: dict
    """
    raw = read_file(path)
    try:
        cfg = yaml.load(raw)
    except yaml.scanner.ScannerError as e:
        raise ConfigurationError(
            'Unable to parse YAML file. Error: {0}'.format(e))
    return cfg

def test_client_options(config):
    """
    Test whether a SSL/TLS files exist. Will raise an exception if the files
    cannot be read.

    :arg config: A client configuration file data dictionary
    :rtype: None
    """
    if config['use_ssl']:
        # Test whether certificate is a valid file path
        if 'certificate' in config and config['certificate']:
            read_file(config['certificate'])
        # Test whether client_cert is a valid file path
        if 'client_cert' in config and config['client_cert']:
            read_file(config['client_cert'])
        # Test whether client_key is a valid file path
        if 'client_key' in config and  config['client_key']:
            read_file(config['client_key'])

def verify_client_object(test):
    """
    Test if `test` is a proper :class:`elasticsearch.Elasticsearch` client
    object and raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    # Ignore mock type for testing
    if str(type(test)) == "<class 'mock.Mock'>":
        pass
    elif not type(test) == type(elasticsearch.Elasticsearch()):
        raise TypeError(
            'Not a client object. Type: {0}'.format(type(test))
        )

def verify_index_list(test):
    """
    Test if `test` is a proper :class:`curator.indexlist.IndexList` object and
    raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    if not str(type(test)) == "<class 'curator.indexlist.IndexList'>":
        raise TypeError(
            'Not an IndexList object. Type: {0}.'.format(type(test))
        )

def verify_snapshot_list(test):
    """
    Test if `test` is a proper :class:`curator.snapshotlist.SnapshotList`
    object and raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    if not str(type(test)) == "<class 'curator.snapshotlist.SnapshotList'>":
        raise TypeError(
            'Not an SnapshotList object. Type: {0}.'.format(type(test))
        )

def report_failure(exception):
    """
    Raise a `FailedExecution` exception and include the original error message.

    :arg exception: The upstream exception.
    :rtype: None
    """
    raise FailedExecution(
        'Exception encountered.  Rerun with loglevel DEBUG and/or check '
        'Elasticsearch logs for more information. '
        'Exception: {0}'.format(exception)
    )

def get_date_regex(timestring):
    """
    Return a regex string based on a provided strftime timestring.

    :arg timestring: An strftime pattern
    :rtype: str
    """
    prev = ''; curr = ''; regex = ''
    for s in range(0, len(timestring)):
        curr = timestring[s]
        if curr == '%':
            pass
        elif curr in DATE_REGEX and prev == '%':
            regex += '\d{' + DATE_REGEX[curr] + '}'
        elif curr in ['.', '-']:
            regex += "\\" + curr
        else:
            regex += curr
        prev = curr
    logger.debug("regex = {0}".format(regex))
    return regex

def get_datetime(index_timestamp, timestring):
    """
    Return the datetime extracted from the index name, which is the index
    creation time.

    :arg index_timestamp: The timestamp extracted from an index name
    :arg timestring: An strftime pattern
    :rtype: :py:class:`datetime.datetime`
    """
    # Compensate for week of year by appending '%w' to the timestring
    # and '1' (Monday) to index_timestamp
    if '%W' in timestring:
        timestring += '%w'
        index_timestamp += '1'
    elif '%U' in timestring:
        timestring += '%w'
        index_timestamp += '1'
    elif '%m' in timestring:
        if not '%d' in timestring:
            timestring += '%d'
            index_timestamp += '1'
    # logger.debug(
    #     'index_timestamp: {0}, timestring: {1}, return value: '
    #     '{2}'.format(
    #         index_timestamp, timestring,
    #         datetime.strptime(index_timestamp, timestring)
    #     )
    # )
    return datetime.strptime(index_timestamp, timestring)

def fix_epoch(epoch):
    """
    Fix value of `epoch` to be epoch, which should be 10 or fewer digits long.

    :arg epoch: An epoch timestamp, in epoch + milliseconds, or microsecond, or
        even nanoseconds.
    :rtype: int
    """
    # No decimals allowed
    epoch = int(epoch)
    # If we're still using this script past January, 2038, we have bigger
    # problems than my hacky math here...
    if len(str(epoch)) <= 10:
        return epoch
    elif len(str(epoch)) == 13:
        return int(epoch/1000)
    elif len(str(epoch)) > 10 and len(str(epoch)) < 13:
        raise ValueError(
            'Unusually formatted epoch timestamp.  '
            'Should be 10, 13, or more digits'
        )
    else:
        orders_of_magnitude = len(str(epoch)) - 10
        powers_of_ten = 10**orders_of_magnitude
        epoch = int(epoch/powers_of_ten)
    return epoch

class TimestringSearch(object):
    """
    An object to allow repetitive search against a string, `searchme`, without
    having to repeatedly recreate the regex.

    :arg timestring: An strftime pattern
    """
    def __init__(self, timestring):
        regex = r'(?P<date>{0})'.format(get_date_regex(timestring))
        self.pattern = re.compile(regex)
        self.timestring = timestring
    def get_epoch(self, searchme):
        """
        Return the epoch timestamp extracted from the `timestring` appearing in
        `searchme`.

        :arg searchme: A string to be searched for a date pattern that matches
            `timestring`
        :rtype: int
        """

        match = self.pattern.search(searchme)
        if match:
            if match.group("date"):
                timestamp = match.group("date")
                return (
                    (get_datetime(timestamp, self.timestring) -
                    datetime(1970,1,1)).total_seconds()
                )

def get_point_of_reference(unit, count, epoch=None):
    """
    Get a point-of-reference timestamp in epoch + milliseconds by deriving
    from a `unit` and a `count`, and an optional reference timestamp, `epoch`

    :arg unit: One of ``seconds``, ``minutes``, ``hours``, ``days``, ``weeks``,
        ``months``, or ``years``.
    :arg unit_count: The number of ``units``. ``unit_count`` * ``unit`` will
        be calculated out to the relative number of seconds.
    :arg epoch: An epoch timestamp used in conjunction with ``unit`` and
        ``unit_count`` to establish a point of reference for calculations.
    :rtype: int
    """
    if unit == 'seconds':
        multiplier = 1
    elif unit == 'minutes':
        multiplier = 60
    elif unit == 'hours':
        multiplier = 3600
    elif unit == 'days':
        multiplier = 3600*24
    elif unit == 'weeks':
        multiplier = 3600*24*7
    elif unit == 'months':
        multiplier = 3600*24*30
    elif unit == 'years':
        multiplier = 3600*24*365
    else:
        raise ValueError('Invalid unit: {0}.'.format(unit))
    # Use this moment as a reference point, if one is not provided.
    if not epoch:
        epoch = time.time()
    epoch = fix_epoch(epoch)
    logger.debug('multiplier = {0}, count = {1}'.format(multiplier, count))
    logger.debug('epoch = {0}'.format(epoch))
    logger.debug('Point of reference = {0}'.format(epoch - multiplier * count))
    return epoch - multiplier * count

def byte_size(num, suffix='B'):
    """
    Return a formatted string indicating the size in bytes, with the proper
    unit, e.g. KB, MB, GB, TB, etc.

    :arg num: The number of byte
    :arg suffix: An arbitrary suffix, like `Bytes`
    :rtype: float
    """
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)

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
        raise TypeError(
            'Passed value: {0} is not a list or a string '
            'but is of type {1}'.format(value, type(value))
        )

def chunk_index_list(indices):
    """
    This utility chunks very large index lists into 3KB chunks
    It measures the size as a csv string, then converts back into a list
    for the return value.

    :arg indices: A list of indices to act on.
    :rtype: list
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

def get_indices(client):
    """
    Get the current list of indices from the cluster.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: list
    """
    try:
        indices = list(
            client.indices.get_settings(
            index='_all', params={'expand_wildcards': 'open,closed'})
        )
        logger.debug("All indices: {0}".format(indices))
        return indices
    except Exception as e:
        raise FailedExecution('Failed to get indices. Error: {0}'.format(e))

def get_version(client):
    """
    Return the ES version number as a tuple.
    Omits trailing tags like -dev, or Beta

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
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

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    my_node_id = list(client.nodes.info('_local')['nodes'])[0]
    master_node_id = client.cluster.state(metric='master_node')['master_node']
    return my_node_id == master_node_id

def check_version(client):
    """
    Verify version is within acceptable range.  Raise an exception if it is not.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: None
    """
    version_number = get_version(client)
    logger.debug(
        'Detected Elasticsearch version '
        '{0}'.format(".".join(map(str,version_number)))
    )
    if version_number >= VERSION_MAX or version_number < VERSION_MIN:
        raise CuratorException(
            'Elasticsearch version {0} incompatible '
            'with this version of Curator '
            '({0})'.format(".".join(map(str,version_number)), __version__)
        )

def check_master(client, master_only=False):
    """
    Check if connected client is the elected master node of the cluster.
    If not, cleanly exit with a log message.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: None
    """
    if master_only and not is_master_node(client):
        logger.info(
            'Master-only flag detected. '
            'Connected to non-master node. Aborting.'
        )
        sys.exit(0)

def get_client(**kwargs):
    """
    Return an :class:`elasticsearch.Elasticsearch` client object using the
    provided parameters. Any of the keyword arguments the
    :class:`elasticsearch.Elasticsearch` client object can receive are valid,
    such as:

    :arg hosts: A list of one or more Elasticsearch client hostnames or IP
        addresses to connect to.  Can send a single host.
    :type port: list
    :arg port: The Elasticsearch client port to connect to.
    :type port: int
    :arg url_prefix: `Optional` url prefix, if needed to reach the Elasticsearch
        API (i.e., it's not at the root level)
    :type url_prefix: str
    :arg use_ssl: Whether to connect to the client via SSL/TLS
    :type use_ssl: bool
    :arg certificate: Path to SSL/TLS certificate
    :arg client_cert: Path to SSL/TLS client certificate (public key)
    :arg client_key: Path to SSL/TLS private key
    :arg aws_key: AWS IAM Access Key (Only used if the :mod:`requests-aws4auth`
        python module is installed)
    :arg aws_secret_key: AWS IAM Secret Access Key (Only used if the
        :mod:`requests-aws4auth` python module is installed)
    :arg aws_region: AWS Region (Only used if the :mod:`requests-aws4auth`
        python module is installed)
    :arg ssl_no_validate: If `True`, do not validate the certificate
        chain.  This is an insecure option and you will see warnings in the
        log output.
    :type ssl_no_validate: bool
    :arg http_auth: Authentication credentials in `user:pass` format.
    :type http_auth: str
    :arg timeout: Number of seconds before the client will timeout.
    :type timeout: int
    :arg master_only: If `True`, the client will `only` connect if the
        endpoint is the elected master node of the cluster.  **This option does
        not work if `hosts` has more than one value.**  It will raise an
        Exception in that case.
    :type master_only: bool
    :rtype: :class:`elasticsearch.Elasticsearch`
    """
    if 'url_prefix' in kwargs:
        if (
                type(kwargs['url_prefix']) == type(None) or
                kwargs['url_prefix'] == "None"
            ):
            kwargs['url_prefix'] = ''
    kwargs['hosts'] = '127.0.0.1' if not 'hosts' in kwargs else kwargs['hosts']
    kwargs['master_only'] = False if not 'master_only' in kwargs \
        else kwargs['master_only']
    kwargs['use_ssl'] = False if not 'use_ssl' in kwargs else kwargs['use_ssl']
    kwargs['ssl_no_validate'] = False if not 'ssl_no_validate' in kwargs \
        else kwargs['ssl_no_validate']
    kwargs['certificate'] = False if not 'certificate' in kwargs \
        else kwargs['certificate']
    kwargs['client_cert'] = False if not 'client_cert' in kwargs \
        else kwargs['client_cert']
    kwargs['client_key'] = False if not 'client_key' in kwargs \
        else kwargs['client_key']
    kwargs['hosts'] = ensure_list(kwargs['hosts'])
    logger.debug("kwargs = {0}".format(kwargs))
    master_only = kwargs.pop('master_only')
    if kwargs['use_ssl']:
        if kwargs['ssl_no_validate']:
            kwargs['verify_certs'] = False # Not needed, but explicitly defined
        else:
            logger.info('Attempting to verify SSL certificate.')
            # If user provides a certificate:
            if kwargs['certificate']:
                kwargs['verify_certs'] = True
                kwargs['ca_certs'] = kwargs['certificate']
            else: # Try to use certifi certificates:
                try:
                    import certifi
                    kwargs['verify_certs'] = True
                    kwargs['ca_certs'] = certifi.where()
                except ImportError:
                    logger.warn('Unable to verify SSL certificate.')
    try:
        from requests_aws4auth import AWS4Auth
        kwargs['aws_key'] = False if not 'aws_key' in kwargs \
            else kwargs['aws_key']
        kwargs['aws_secret_key'] = False if not 'aws_secret_key' in kwargs \
            else kwargs['aws_secret_key']
        kwargs['region'] = False if not 'region' in kwargs \
            else kwargs['region']
        if kwargs['aws_key'] or kwargs['aws_secret_key'] or kwargs['region']:
            if not kwargs['aws_key'] and kwargs['aws_secret_key'] \
                    and kwargs['region']:
                raise MissingArgument(
                    'Missing one or more of "aws_key", "aws_secret_key", '
                    'or "region".'
                )
            # Override these kwargs
            kwargs['use_ssl'] = True
            kwargs['verify_certs'] = True
            kwargs['connection_class'] = elasticsearch.RequestsHttpConnection
            kwargs['http_auth'] = (
                AWS4Auth(
                    kwargs['aws_key'], kwargs['aws_secret_key'],
                    kwargs['region'], 'es')
            )
        else:
            logger.debug('"requests_aws4auth" module present, but not used.')
    except ImportError:
        logger.debug('Not using "requests_aws4auth" python module to connect.')

    if master_only:
        if len(kwargs['hosts']) > 1:
            raise ConfigurationError(
                '"master_only" cannot be True if more than one host is '
                'specified. Hosts = {0}'.format(kwargs['hosts'])
            )
    try:
        client = elasticsearch.Elasticsearch(**kwargs)
        # Verify the version is acceptable.
        check_version(client)
        # Verify "master_only" status, if applicable
        check_master(client, master_only=master_only)
        return client
    except Exception as e:
        raise elasticsearch.ElasticsearchException(
            'Unable to create client connection to Elasticsearch.  '
            'Error: {0}'.format(e)
        )

def override_timeout(timeout, action):
    """
    Override the default timeout for `forcemerge`, `snapshot`, and `sync_flush`
    operations if the default value of ``30`` is provided.

    :arg timeout: Number of seconds before the client will timeout.
    :arg action: The `action` to be performed.
    """
    retval = timeout
    if action in ['forcemerge', 'snapshot', 'sync_flush']:
        # Check for default timeout of 30s
        if timeout == 30:
            if action in ['forcemerge', 'snapshot']:
                retval = 21600
            elif action == 'sync_flush':
                retval = 180
            logger.info(
                'Overriding default connection timeout for {0} action.  '
                'New timeout: {1}'.format(action.upper(),timeout)
            )
    return retval

def show_dry_run(ilo, action, **kwargs):
    """
    Log dry run output with the action which would have been executed.

    :arg ilo: A :class:`curator.indexlist.IndexList`
    :arg action: The `action` to be performed.
    :arg kwargs: Any other args to show in the log output
    """
    logger.info('DRY-RUN MODE.  No changes will be made.')
    logger.info(
        '(CLOSED) indices may be shown that may not be acted on by '
        'action "{0}".'.format(action)
    )
    indices = sorted(ilo.indices)
    for idx in indices:
            index_closed = ilo.index_info[idx]['state'] == 'close'
            logger.info(
                'DRY-RUN: {0}: {1}{2} with arguments: {3}'.format(
                    action, idx, ' (CLOSED)' if index_closed else '', kwargs
                )
            )

### SNAPSHOT STUFF ###
def get_repository(client, repository=''):
    """
    Return configuration information for the indicated repository.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: dict
    """
    try:
        return client.snapshot.get_repository(repository=repository)
    except (elasticsearch.TransportError, elasticsearch.NotFoundError):
        logger.error("Repository {0} not found.".format(repository))
        return False

def get_snapshot(client, repository=None, snapshot=''):
    """
    Return information about a snapshot (or a comma-separated list of snapshots)
    If no snapshot specified, it will return all snapshots.  If none exist, an
    empty dictionary will be returned.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name, or a comma-separated list of snapshots
    :rtype: dict
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    snapname = '_all' if snapshot == '' else snapshot
    try:
        return client.snapshot.get(repository=repository, snapshot=snapshot)
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as e:
        raise FailedExecution(
            'Unable to get information about snapshot {0} from repository: '
            '{1}.  Error: {2}'.format(snapname, repository, e)
        )

def get_snapshot_data(client, repository=None):
    """
    Get ``_all`` snapshots from repository and return a list.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: list
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    try:
        return client.snapshot.get(
            repository=repository, snapshot="_all")['snapshots']
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as e:
        raise FailedExecution(
            'Unable to get snapshot information from repository: {0}.  '
            'Error: {1}'.format(repository, e)
        )

def snapshot_in_progress(client, repository=None, snapshot=None):
    """
    Determine whether the provided snapshot in `repository` is ``IN_PROGRESS``.
    If no value is provided for `snapshot`, then check all of them.
    Return `snapshot` if it is found to be in progress, or `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name
    """
    allsnaps = get_snapshot_data(client, repository=repository)
    inprogress = (
        [snap['snapshot'] for snap in allsnaps if 'state' in snap.keys() \
            and snap['state'] == 'IN_PROGRESS']
    )
    if snapshot:
        return snapshot if snapshot in inprogress else False
    else:
        if len(inprogress) == 0:
            return False
        elif len(inprogress) == 1:
            return inprogress[0]
        else: # This should not be possible
            raise CuratorException(
                'More than 1 snapshot in progress: {0}'.format(inprogress)
            )

def safe_to_snap(client, repository=None, retry_interval=120, retry_count=3):
    """
    Ensure there are no snapshots in progress.  Pause and retry accordingly

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg retry_interval: Number of seconds to delay betwen retries. Default:
        120 (seconds)
    :arg retry_count: Number of attempts to make. Default: 3
    :rtype: bool
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    for count in range(1, retry_count+1):
        in_progress = snapshot_in_progress(
            client, repository=repository
        )
        if in_progress:
            logger.info(
                'Snapshot already in progress: {0}'.format(in_progress))
            logger.info(
                'Pausing {0} seconds before retrying...'.format(
                    retry_interval)
            )
            time.sleep(retry_interval)
            logger.info('Retry {0} of {1}'.format(count, retry_count))
        else:
            return True
    return False


def create_snapshot_body(indices, ignore_unavailable=False,
                         include_global_state=True, partial=False):
    """
    Create the request body for creating a snapshot from the provided
    arguments.

    :arg indices: A single index, or list of indices to snapshot.
    :arg ignore_unavailable: Ignore unavailable shards/indices. (default:
        `False`)
    :type ignore_unavailable: bool
    :arg include_global_state: Store cluster global state with snapshot.
        (default: `True`)
    :type include_global_state: bool
    :arg partial: Do not fail if primary shard is unavailable. (default:
        `False`)
    :type partial: bool
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
        raise MissingArgument('Missing required parameter --repo_type')

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

    :arg client: An :class:`elasticsearch.Elasticsearch` client object

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
    :rtype: bool
    """
    if not 'repository' in kwargs:
        raise MissingArgument('Missing required parameter --repository')
    else:
        repository = kwargs['repository']

    try:
        body = create_repo_body(**kwargs)
        logger.info(
            'Checking if repository {0} already exists...'.format(repository)
        )
        result = get_repository(client, repository=repository)
        logger.debug("Result = {0}".format(result))
        if not result:
            logger.info(
                'Repository {0} not in Elasticsearch. Continuing...'.format(
                    repository
                )
            )
            client.snapshot.create_repository(repository=repository, body=body)
        elif result is not None and repository not in result:
            logger.info(
                'Repository {0} not in Elasticsearch. Continuing...'.format(
                    repository
                )
            )
            client.snapshot.create_repository(repository=repository, body=body)
        else:
            raise FailedExecution(
                'Unable to create repository {0}.  A repository with that name '
                'already exists.'.format(repository)
            )
    except elasticsearch.TransportError as e:
        raise FailedExecution(
            """
            Unable to create repository {0}.  Response Code: {1}.  Error: {2}.
            Check curator and elasticsearch logs for more information.
            """.format(
                repository, e.status_code, e.error
                )
        )
    logger.info("Repository {0} creation initiated...".format(repository))
    return True

def repository_exists(client, repository=None):
    """
    Verify the existence of a repository

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: bool
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    test_result = get_repository(client, repository)
    if repository in test_result:
        logger.info("Repository {0} exists.".format(repository))
        return True
    else:
        logger.info("Repository {0} not found...".format(repository))
        return False

def test_repo_fs(client, repository=None):
    """
    Test whether all nodes have write access to the repository

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    """
    try:
        nodes = client.snapshot.verify_repository(
            repository=repository)['nodes']
        logger.info('All nodes can write to the repository')
        logger.debug(
            'Nodes with verified repository access: {0}'.format(nodes))
    except Exception as e:
        try:
            if e.status_code == 404:
                msg = (
                    '--- Repository "{0}" not found. Error: '
                    '{1}, {2}'.format(repository, e.status_code, e.error)
                )
            else:
                msg = (
                    '--- Got a {0} response from Elasticsearch.  '
                    'Error message: {1}'.format(e.status_code, e.error)
                )
        except AttributeError:
            msg = ('--- Error message: {0}'.format(e))
        raise ActionError(
            'Failed to verify all nodes have repository access: '
            '{0}'.format(msg)
        )

def snapshot_running(client):
    """
    Return `True` if a snapshot is in progress, and `False` if not

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    try:
        status = client.snapshot.status()['snapshots']
    except Exception as e:
        report_failure(e)
    # We will only accept a positively identified False.  Anything else is
    # suspect.
    return False if status == [] else True

def parse_date_pattern(name):
    """
    Scan and parse `name` for :py:func:`time.strftime` strings, replacing them
    with the associated value when found, but otherwise returning lowercase
    values, as uppercase snapshot names are not allowed.

    The :py:func:`time.strftime` identifiers that Curator currently recognizes
    as acceptable include:

    * ``Y``: A 4 digit year
    * ``y``: A 2 digit year
    * ``m``: The 2 digit month
    * ``W``: The 2 digit week of the year
    * ``d``: The 2 digit day of the month
    * ``H``: The 2 digit hour of the day, in 24 hour notation
    * ``M``: The 2 digit minute of the hour
    * ``S``: The 2 digit number of second of the minute
    * ``j``: The 3 digit day of the year

    :arg name: A name, which can contain :py:func:`time.strftime`
        strings
    """
    prev = ''; curr = ''; rendered = ''
    for s in range(0, len(name)):
        curr = name[s]
        if curr == '%':
            pass
        elif curr in DATE_REGEX and prev == '%':
            rendered += str(datetime.utcnow().strftime('%{0}'.format(curr)))
        else:
            rendered += curr
        logger.debug('Partially rendered name: {0}'.format(rendered))
        prev = curr
    logger.debug('Fully rendered name: {0}'.format(rendered))
    return rendered

def prune_nones(mydict):
    """
    Remove keys from `mydict` whose values are `None`

    :arg mydict: The dictionary to act on
    :rtype: dict
    """
    # Must test for None, because 0 is acceptable.
    return dict([(k,v) for k, v in mydict.items() if v != None and v != 'None'])

def verify_args(action, options):
    """
    Verify that only acceptable argument names in `options` have been passed for
    any given `action`

    :arg action: The name of an action to be performed
    :arg options: A dictionary of options.
    """
    logger.debug('Arguments for action "{0}": {1}'.format(action, options))
    def matches_keys(mydict):
        logger.debug(
            'options.keys = {0} mydict.keys = '
            '{1}'.format(options.keys(), mydict.keys())
        )
        return sorted(list(options.keys())) == sorted(list(mydict.keys()))

    # deepcopy guarantees clean copies of the defaults, and nothing getting
    # altered in "pass by reference," which was happening in testing.
    if not matches_keys(copy.deepcopy(ACTION_DEFAULTS[action])):
        raise ConfigurationError(
            'Invalid option in configuration: {0}'.format(options))
