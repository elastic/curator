#!/usr/bin/env python
#
# Time-based and Size-based operations are mutually exclusive!
#
# Time-based operations
# Put an integer (n) after any of the following options to:
# --delete, --close, (disable) --bloom (filters), --optimize
# Do one or more of these on indices older than (n) --time-unit(s)
# where --time-unit can be 'days' or 'hours'
#
# Size-based operations
# --disk-space (float)
# Permits deletion of indices in excess of (float) size in GB
#
# This script presumes an index is named typically, e.g. logstash-YYYY.MM.DD
# It will work with any ${prefix}YYYY.MM.DD or ${prefix}YYYY.MM.DD.HH sequence
# where --prefix defines the variable ${prefix}, with a default of logstash-
#
# REQUIREMENTS
# Requires python and the following dependencies (all pip/easy_installable):
#
# elasticsearch (official Elasticsearch Python API,
# http://www.elasticsearch.org/guide/en/elasticsearch/client/python-api/current/index.html)
#
# argparse (built-in in python2.7 and higher, python 2.6 and lower will have to
# easy_install it)
#
# TODO: Unit tests. The code is somewhat broken up into logical parts that may
#       be tested separately.
#       Make sure the code can be used outside of __main__ by people importing the module
#       Better error reporting?
#       Improve the get_index_epoch method to parse more date formats. Consider renaming (to "parse_date_to_timestamp"?)

import sys
import time
import logging
from datetime import timedelta, datetime

import elasticsearch

# This solves https://github.com/elasticsearch/curator/issues/12
try:
    from logging import NullHandler
except ImportError:
    from logging import Handler

    class NullHandler(Handler):
        def emit(self, record):
            pass

__version__ = '1.1.0-dev'

# Elasticsearch versions supported
version_max  = (2, 0, 0)
version_min = (1, 0, 0)
        
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'host': 'localhost',
    'url_prefix': '',
    'port': 9200,
    'ssl': False,
    'timeout': 30,
    'prefix': 'logstash-',
    'separator': '.',
    'curation_style': 'time',
    'time_unit': 'days',

    'max_num_segments': 2,
    'dry_run': False,
    'debug': False,
    'log_level': 'INFO',
    'show_indices': False,
    
    'repo_type': 'fs',
    'create_repo': False,
    'delete_repo': False,
    'bucket': None,
    'region': None,
    'base_path': None,
    'access_key': None,
    'secret_key': None,
    'compress': True,
    'concurrent_streams': 5,
    'chunk_size': None,
    'max_restore_bytes_per_sec': None,
    'max_snapshot_bytes_per_sec': None,
    'wait_for_completion': True,
    'ignore_unavailable': False,
    'include_global_state': False,
    'partial': False,
}

def make_parser():
    """ Creates an ArgumentParser to parse the command line options. """
    help_desc = 'Curator for Elasticsearch indices.  Can delete (by space or time), close, disable bloom filters and optimize (forceMerge) your indices.'
    try:
        import argparse
        parser = argparse.ArgumentParser(description=help_desc)
        parser.add_argument('-v', '--version', action='version', version='%(prog)s '+__version__)
    except ImportError:
        import optparse
        parser = optparse.OptionParser(description=help_desc, version='%prog '+ __version__)
        parser.parse_args_orig = parser.parse_args
        parser.parse_args = lambda: parser.parse_args_orig()[0]
        parser.add_argument = parser.add_option
    parser.add_argument('--host', help='Elasticsearch host. Default: localhost', default=DEFAULT_ARGS['host'])
    parser.add_argument('--url_prefix', help='Elasticsearch http url prefix. Default: none', default=DEFAULT_ARGS['url_prefix'])
    parser.add_argument('--port', help='Elasticsearch port. Default: 9200', default=DEFAULT_ARGS['port'], type=int)
    parser.add_argument('--ssl', help='Connect to Elasticsearch through SSL. Default: false', action='store_true', default=DEFAULT_ARGS['ssl'])
    parser.add_argument('-t', '--timeout', help='Connection timeout in seconds. Default: 30', default=DEFAULT_ARGS['timeout'], type=int)

    parser.add_argument('-p', '--prefix', help='Prefix for the indices. Indices that do not have this prefix are skipped. Default: logstash-', default=DEFAULT_ARGS['prefix'])
    parser.add_argument('-s', '--separator', help='TIME_UNIT separator. Default: .', default=DEFAULT_ARGS['separator'])

    parser.add_argument('-C', '--curation-style', dest='curation_style', action='store', help='Curate indices by [time, space] Default: time', default=DEFAULT_ARGS['curation_style'], type=str)
    parser.add_argument('-T', '--time-unit', dest='time_unit', action='store', help='Unit of time to reckon by: [days, hours] Default: days', default=DEFAULT_ARGS['time_unit'], type=str)
    # Standard features
    parser.add_argument('-d', '--delete', dest='delete_older', action='store', help='Delete indices older than DELETE_OLDER TIME_UNITs.', type=int)
    parser.add_argument('-c', '--close', dest='close_older', action='store', help='Close indices older than CLOSE_OLDER TIME_UNITs.', type=int)
    parser.add_argument('-b', '--bloom', dest='bloom_older', action='store', help='Disable bloom filter for indices older than BLOOM_OLDER TIME_UNITs.', type=int)
    parser.add_argument('-g', '--disk-space', dest='disk_space', action='store', help='Delete indices beyond DISK_SPACE gigabytes.', type=float)
    # Index routing
    parser.add_argument('-r', '--require', help='Update indices required routing allocation rules. Ex. tag=ssd', type=int)
    parser.add_argument('--required_rule', help='Index routing allocation rule to require. Ex. tag=ssd', type=str)
    # Optimize
    parser.add_argument('--max_num_segments', action='store', help='Maximum number of segments, post-optimize. Default: 2', type=int, default=DEFAULT_ARGS['max_num_segments'])
    parser.add_argument('-o', '--optimize', action='store', help='Optimize (Lucene forceMerge) indices older than OPTIMIZE TIME_UNITs.  Must increase timeout to stay connected throughout optimize operation, recommend no less than 3600.', type=int)
    # Meta-data
    parser.add_argument('-n', '--dry-run', action='store_true', help='If true, does not perform any changes to the Elasticsearch indices.', default=DEFAULT_ARGS['dry_run'])
    parser.add_argument('-D', '--debug', dest='debug', action='store_true', help='Debug mode', default=DEFAULT_ARGS['debug'])
    parser.add_argument('-ll', '--loglevel', dest='log_level', action='store', help='Log level', default=DEFAULT_ARGS['log_level'], type=str)
    parser.add_argument('-l', '--logfile', dest='log_file', help='log file', type=str)
    parser.add_argument('--show-indices', dest='show_indices', action='store_true', help='Show indices matching prefix (nullifies other operations)', default=DEFAULT_ARGS['show_indices'])
    # Snapshot
    parser.add_argument('--snap-older', dest='snap_older', action='store', type=int,
                        help='[Snapshot] indices older than SNAP_OLDER TIME_UNITs.')
    parser.add_argument('--snap-latest', dest='snap_latest', action='store', type=int,
                        help='[Snapshot] Capture most recent (SNAP_LATEST) number of indices matching PREFIX.')
    parser.add_argument('--delete-snaps', dest='delete_snaps', action='store', type=int,
                        help='[Snapshot] delete snapshots older than DELETE_SNAPS TIME_UNITs.')
    parser.add_argument('--no_wait_for_completion', dest='wait_for_completion', action='store_false',
                        help='[Snapshot] Do not wait until complete to return. Waits by default.', default=DEFAULT_ARGS['wait_for_completion'])
    parser.add_argument('--ignore_unavailable', dest='ignore_unavailable', action='store_true',
                        help='[Snapshot] Ignore unavailable shards/indices. (Default=False)', default=DEFAULT_ARGS['ignore_unavailable'])
    parser.add_argument('--include_global_state', dest='include_global_state', action='store_true',
                        help='[Snapshot] Store cluster global state with snapshot. (Default=False)', default=DEFAULT_ARGS['include_global_state'])
    parser.add_argument('--partial', dest='partial', action='store_true',
                        help='[Snapshot] Do not fail if primary shard is unavailable. (Default=False)', default=DEFAULT_ARGS['partial'])
    parser.add_argument('--show-repositories', dest='show_repositories', action='store_true',
                        help='[Snapshot] Show all registed repositories.')
    parser.add_argument('--show-snapshots', dest='show_snapshots', action='store_true',
                        help='[Snapshot] Show all snapshots in REPOSITORY.')
    # Repository creation
    parser.add_argument('--repository', dest='repository', action='store', type=str,
                        help='[Snapshot] Repository name')
    parser.add_argument('--create-repo', dest='create_repo', action='store_true', default=DEFAULT_ARGS['create_repo'],
                        help='[Snapshot] Create indicated (--repository)')
    parser.add_argument('--delete-repo', dest='delete_repo', action='store_true', default=DEFAULT_ARGS['delete_repo'],
                        help='[Snapshot] Create indicated (--repository)')
    parser.add_argument('--repo-type', dest='repo_type', action='store', type=str, default=DEFAULT_ARGS['repo_type'],
                        help='[Snapshot] Repository type, one of "fs", "s3"')
    parser.add_argument('--disable-compression', dest='compress', action='store_false', default=DEFAULT_ARGS['compress'],
                        help='[Snapshot] Disable compression (enabled by default)')
    parser.add_argument('--concurrent_streams', dest='concurrent_streams', action='store', type=int, default=DEFAULT_ARGS['concurrent_streams'],
                        help='[Snapshot] Number of streams (per node) preforming snapshot. Default: 5')
    parser.add_argument('--chunk_size', dest='chunk_size', action='store', type=str, default=DEFAULT_ARGS['chunk_size'],
                        help='[Snapshot] Chunk size, e.g. 1g, 10m, 5k. Default is unbounded.')
    parser.add_argument('--max_restore_bytes_per_sec', dest='max_restore_bytes_per_sec', action='store', type=str, default=DEFAULT_ARGS['max_restore_bytes_per_sec'],
                        help='[Snapshot] Throttles per node restore rate (per second). Default: 20mb')
    parser.add_argument('--max_snapshot_bytes_per_sec', dest='max_snapshot_bytes_per_sec', action='store', type=str, default=DEFAULT_ARGS['max_snapshot_bytes_per_sec'],
                        help='[Snapshot] Throttles per node snapshot rate (per second). Default: 20mb')
    # 'fs' repository args
    parser.add_argument('--location', dest='location', action='store', type=str, default=None,
                        help='[Snapshot][fs] Shared file-system location. Must match remote path, & be accessible to all master & data nodes')
    # 's3' repository args
    parser.add_argument('--bucket', dest='bucket', action='store', type=str, default=DEFAULT_ARGS['bucket'],
                        help='[Snapshot][s3] Repository bucket name')
    parser.add_argument('--region', dest='region', action='store', type=str, default=DEFAULT_ARGS['region'],
                        help='[Snapshot][s3] S3 region. Defaults to US Standard')
    parser.add_argument('--base_path', dest='base_path', action='store', type=str, default=DEFAULT_ARGS['base_path'],
                        help='[Snapshot][s3] S3 base path. Defaults to root directory.')
    parser.add_argument('--access_key', dest='access_key', action='store', type=str, default=DEFAULT_ARGS['access_key'],
                        help='[Snapshot][s3] S3 access key. Defaults to value of cloud.aws.access_key')
    parser.add_argument('--secret_key', dest='secret_key', action='store', type=str, default=DEFAULT_ARGS['secret_key'],
                        help='[Snapshot][s3] S3 secret key. Defaults to value of cloud.aws.secret_key')
    return parser


def validate_args(myargs):
    """Validate that arguments aren't stomping on each other or conflicting"""
    success = True
    messages = []
    if myargs.curation_style == 'time':
        if myargs.time_unit != 'days' and myargs.time_unit != 'hours':
            success = False
            messages.append('Values for --time-unit must be either "days" or "hours"')
            if myargs.disk_space:
                success = False
                messages.append('Cannot specify --disk-space and --curation-style "time"')
        if not myargs.create_repo:
            if not myargs.delete_older and not myargs.close_older and not myargs.bloom_older and not myargs.optimize and not myargs.require and not myargs.snap_older and not myargs.snap_latest and not myargs.delete_snaps and not myargs.delete_repo:
                success = False
                messages.append('Must specify at least one of --delete, --close, --bloom, --optimize, --require, --snap-older, --snap-latest, --delete-snaps, or --delete-repo')
            if ((myargs.delete_older and myargs.delete_older < 1) or
                (myargs.close_older  and myargs.close_older  < 1) or
                (myargs.bloom_older  and myargs.bloom_older  < 1) or
                (myargs.optimize     and myargs.optimize     < 1) or
                (myargs.snap_older   and myargs.snap_older   < 1)):
                success = False
                messages.append('Values for --delete, --close, --bloom, --optimize, or --snap_older must be > 0')
            if myargs.optimize and myargs.timeout < 300:
                success = False
                messages.append('Timeout should be much higher for optimize transactions. Recommend no less than 3600 seconds')
        else: # myargs.create_repo is True
            if not myargs.repository and not myargs.repo_type:
                success = False
                messages.append('Cannot create repository without --repository and --repo_type arguments')
            if myargs.repo_type not in ['fs', 's3']:
                success = False
                messages.append('Incorrect repository type.')
            if myargs.repo_type == 'fs' and not myargs.location:
                success = False
                messages.append('Need to include --location with \'--repo-type fs\'.')
            if myargs.repo_type == 's3' and not myargs.bucket:
                success = False
                messages.append('Need to include --bucket with \'--repo-type s3\'.')
        if myargs.snap_older and not myargs.repository:
            success = False
            messages.append('Cannot create snapshot without both --repository and --snap-older')
        if myargs.snap_older and myargs.timeout < 300:
            success = False
            messages.append('Timeout should be much higher for snapshot operations. Recommend no less than 3600 seconds')
        if myargs.snap_latest and myargs.timeout < 300:
            success = False
            messages.append('Timeout should be much higher for snapshot operations. Recommend no less than 3600 seconds')
    else: # Curation-style is 'space'
        if (myargs.delete_older or myargs.close_older or myargs.bloom_older or myargs.optimize or myargs.repository or myargs.snap_older):
            success = False
            messages.append('Cannot specify --curation-style "space" and any of --delete, --close, --bloom, --optimize, --repository, or --snap-older')
        if (myargs.disk_space == 0) or (myargs.disk_space < 0):
            success = False
            messages.append('Value for --disk-space must be greater than 0')
    if success:
        return True
    else:
        return messages

def get_index_time(index_timestamp, separator='.'):
    """ Gets the time of the index.

    :param index_timestamp: A string on the format YYYY.MM.DD[.HH]
    :return The creation time (datetime) of the index.
    """
    try:
        return datetime.strptime(index_timestamp, separator.join(('%Y', '%m', '%d', '%H')))
    except ValueError:
        return datetime.strptime(index_timestamp, separator.join(('%Y', '%m', '%d')))

def get_indices(client, prefix='logstash-'):
    """Return a sorted list of indices matching prefix"""
    return sorted(client.indices.get_settings(index=prefix+'*', params={'expand_wildcards': 'closed'}).keys())
    
def get_snaplist(client, repo_name, prefix='logstash-'):
    """Get _all snapshots containing prefix from repo_name and return a list"""
    retval = []
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        retval = [snap['snapshot'] for snap in allsnaps if 'snapshot' in snap.keys()]
        retval = [i for i in retval if prefix in i]
    except:
        logger.warn("No snapshots found.")
    return retval

def get_snapped_indices(client, repo_name, prefix='logstash-'):
    """Return all indices in snapshots which succeeded and match prefix"""
    retval = []
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        succeeded = [snap['indices'] for snap in allsnaps if 'snapshot' in snap.keys() and snap["state"] == "SUCCESS"]
        for i in succeeded:
            retval += i
        retval = [i for i in retval if prefix in i]
    except:
        logger.warn("No snapshots found.")
    return list(set(retval))

def get_version(client):
    """Return ES version number as a tuple"""
    version = client.info()['version']['number']
    return tuple(map(int, version.split('.')))

def find_expired_data(client, time_unit, unit_count, data_type='index', repo_name=None, separator='.', prefix='logstash-', utc_now=None):
    """ Generator that yields expired objects (indices or snapshots).

    :return: Yields tuples on the format ``(name, expired_by)`` where name
        is the name of the expired object and expired_by is the interval (timedelta) that the
        object was expired by.
    """
    # time-injection for test purposes only
    utc_now = utc_now if utc_now else datetime.utcnow()
    # reset to start of the period to be sure we are not retiring a human by mistake
    utc_now = utc_now.replace(minute=0, second=0, microsecond=0)

    if time_unit == 'hours':
        required_parts = 4
    else:
        required_parts = 3
        utc_now = utc_now.replace(hour=0)

    cutoff = utc_now - timedelta(**{time_unit: (unit_count - 1)})
    if data_type == 'index':
        object_list = get_indices(client, prefix)
    elif data_type == 'snapshot':
        if repo_name:
            object_list = get_snaplist(client, repo_name)
        else:
            logger.error('Repository name not specified.')
            return

    for object_name in object_list:

        unprefixed_object_name = object_name[len(prefix):]

        # find the timestamp parts (i.e ['2011', '01', '05'] from '2011.01.05') using the configured separator
        parts = unprefixed_object_name.split(separator)

        # verify we have a valid cutoff - hours for 4-part indices, days for 3-part
        if len(parts) != required_parts:
            logger.debug('Skipping {0} because it is of a type (hourly or daily) that I\'m not asked to evaluate.'.format(object_name))
            continue

        try:
            object_time = get_index_time(unprefixed_object_name, separator=separator)
        except ValueError:
            logger.error('Could not find a valid timestamp from the {0}: {1}'.format(data_type, object_name))
            continue

        # if the index is older than the cutoff
        if object_time < cutoff:
            yield object_name, cutoff-object_time

        else:
            logger.info('{0} is {1} above the cutoff.'.format(object_name, object_time-cutoff))

def find_overusage_indices(client, disk_space_to_keep, separator='.', prefix='logstash-'):
    """ Generator that yields over usage indices.

    :return: Yields tuples on the format ``(index_name, 0)`` where index_name
    is the name of the expired index. The second element is only here for
    compatiblity reasons.
    """

    disk_usage = 0.0
    disk_limit = disk_space_to_keep * 2**30

    stats = client.indices.status(index=prefix+'*')
    sorted_indices = sorted(
        (
            (index_name, index_stats['index']['primary_size_in_bytes'])
            for (index_name, index_stats) in stats['indices'].items()
        ),
        reverse=True
    )

    for index_name, index_size in sorted_indices:
        disk_usage += index_size

        if disk_usage > disk_limit:
            yield index_name, 0
        else:
            logger.info('skipping {0}, disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))

def index_closed(client, index_name):
    """Return True if index is closed"""
    index_metadata = client.cluster.state(
        index=index_name,
        metric='metadata',
    )
    return index_metadata['metadata']['indices'][index_name]['state'] == 'close'
    
def _get_repository(client, repo_name):
    """Get Snapshot Repository information"""
    try:
        return client.snapshot.get_repository(repository=repo_name)
    except Exception as e:
        if e.status_code == 404:
            logger.info("Repository {0} not found".format(repo_name))
            return None

def _create_repository(client, repo_name, body):
    """Create repository with repo_name and body settings"""
    try:
        result = _get_repository(client, repo_name)
        if not result:
            client.snapshot.create_repository(repository=repo_name, body=body)
        elif result is not None and repo_name not in result:
            client.snapshot.create_repository(repository=repo_name, body=body)
        else:
            logger.error("Unable to create repository {0}.  A repository with that name already exists.".format(repo_name))
    except:
        logger.error("Unable to create repository {0}.  Check logs for more information.".format(repo_name))
        return False
    logger.info("Repository {0} creation initiated...".format(repo_name))
    test_result = _get_repository(client, repo_name)
    if repo_name in test_result:
        logger.info("Repository {0} created successfully.".format(repo_name))
        return True
    else:
        logger.error("Repository {0} failed validation...".format(repo_name))
        return False

def _delete_repository(client, repo_name):
    """Delete repository with repo_name"""
    return client.snapshot.delete_repository(repository=repo_name)

def _get_snapshot(client, repo_name, snap_name):
    """Get information about a snapshot (or snapshots)"""
    return client.snapshot.get(repository=repo_name, snapshot=snap_name)

def _create_snapshot(client, snap_name, prefix='logstash-', **kwargs):
    """Create a snapshot (or snapshots). Overwrite failures
        'argdict' is passed in kwargs in this method so it can work in the index_loop
    """
    # Return True when it was skipped
    try:
        argdict = kwargs['argdict']
        repo_name = argdict['repository']
        successes = get_snapped_indices(client, repo_name, prefix=prefix)
        snaps = get_snaplist(client, repo_name, prefix=prefix)
        logger.debug("successes = {0}".format(successes))
        logger.debug("snaps = {0}".format(snaps))
        if not snap_name in snaps and not snap_name in successes and not index_closed(client, snap_name):
            client.snapshot.create(repository=repo_name, snapshot=snap_name, body=create_snapshot_body(snap_name, argdict), wait_for_completion=argdict['wait_for_completion'])
        elif snap_name in snaps and not snap_name in successes and not index_closed(client, snap_name):
            logger.warn("Previous snapshot was unsuccessful.  Deleting snapshot {0} and trying again.".format(snap_name))
            _delete_snapshot(client, repo_name, snap_name)
            client.snapshot.create(repository=repo_name, snapshot=snap_name, body=create_snapshot_body(snap_name, argdict), wait_for_completion=argdict['wait_for_completion'])
        else:
            logger.info("Skipping: A snapshot with name '{0}' already exists.".format(snap_name))
            return True
    except Exception as e:
        logger.error("Unable to create snapshot {0}.  Error: {1} Check logs for more information.".format(snap_name, e))
        return True

def _delete_snapshot(client, snap_name, **kwargs):
    """Delete a snapshot (or snapshots)"""
    # kwargs is here to preserve expected number of args passed by index_loop
    client.snapshot.delete(repository=kwargs['repo_name'], snapshot=snap_name)
    
def _close_index(client, index_name, **kwargs):
    if index_closed(client, index_name):
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.close(index=index_name)

def _delete_index(client, index_name, **kwargs):
    client.indices.delete(index=index_name)

def _optimize_index(client, index_name, max_num_segments=2, **kwargs):
    if index_closed(client, index_name): # Don't try to optimize a closed index
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        shards, segmentcount = get_segmentcount(client, index_name)
        logger.debug('Index {0} has {1} shards and {2} segments total.'.format(index_name, shards, segmentcount))
        if segmentcount > (shards * max_num_segments):
            logger.info('Optimizing index {0} to {1} segments per shard.  Please wait...'.format(index_name, max_num_segments))
            client.indices.optimize(index=index_name, max_num_segments=max_num_segments)
        else:
            logger.info('Skipping index {0}: Already optimized.'.format(index_name))
            return True

def _bloom_index(client, index_name, **kwargs):
    if index_closed(client, index_name): # Don't try to disable bloom filter on a closed index.  It will re-open them
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.put_settings(index=index_name, body='index.codec.bloom.load=false')
        
def _require_index(client, index_name, attr, **kwargs):
    key = attr.split('=')[0]
    value = attr.split('=')[1]
    if index_closed(client, index_name):
      logger.info('Skipping index {0}: Already closed.'.format(index_name))
      return True
    else:
      logger.info('Updating index setting index.routing.allocation.{0}={1}'.format(key,value))
      client.indices.put_settings(index=index_name, body='index.routing.allocation.{0}={1}'.format(key,value))

OP_MAP = {
    'close'       : (_close_index, {'op': 'close', 'verbed': 'closed', 'gerund': 'Closing'}),
    'delete'      : (_delete_index, {'op': 'delete', 'verbed': 'deleted', 'gerund': 'Deleting'}),
    'optimize'    : (_optimize_index, {'op': 'optimize', 'verbed': 'optimized', 'gerund': 'Optimizing'}),
    'bloom'       : (_bloom_index, {'op': 'disable bloom filter for', 'verbed': 'bloom filter disabled', 'gerund': 'Disabling bloom filter for'}),
    'require'     : (_require_index, {'op': 'update require allocation rules for', 'verbed':'index routing allocation updated', 'gerund': 'Updating required index routing allocation rules for'}),
    'snapshot'    : (_create_snapshot, {'op': 'create snapshot for', 'verbed':'created snapshot', 'gerund': 'Initiating snapshot for'}),
    'delete_snaps': (_delete_snapshot, {'op': 'delete snapshot for', 'verbed':'deleted snapshot', 'gerund': 'Deleting snapshot for'}),
}

def snap_latest_indices(client, count, prefix='logstash-', dry_run=False, **kwargs):
    """Snapshot 'count' most recent indices matching prefix
       'argdict' must be in kwargs for this to work
    """
    indices = [] # initialize...
    indices = get_indices(client, prefix)
    for index_name in indices[-count:]:
        if dry_run:
            logger.info('Would have attempted creating snapshot for {0}.'.format(index_name))
            continue
        else:
            if not index_closed(client, index_name):
                logger.info('Attempting to create snapshot for {0}...'.format(index_name))
            else:
                logger.warn('Unable to perform snapshot on closed index {0}'.format(index_name))
                continue
        
        skipped = _create_snapshot(client, index_name, prefix, **kwargs)
            
        if skipped:
            continue
        # if no error was raised and we got here that means the operation succeeded
        logger.info('Snapshot operation for index {0} succeeded.'.format(index_name))
    logger.info('Snapshot \'latest\' {0} indices operations completed.'.format(count))

def index_loop(client, operation, expired_indices, dry_run=False, by_space=False, **kwargs):
    op, words = OP_MAP[operation]
    for index_name, expiration in expired_indices:
        if dry_run and not by_space:
            logger.info('Would have attempted {0} index {1} because it is {2} older than the calculated cutoff.'.format(words['gerund'].lower(), index_name, expiration))
            continue
        elif dry_run and by_space:
            logger.info('Would have attempted {0} index {1} due to space constraints.'.format(words['gerund'].lower(), index_name))
            continue

        if not by_space:
            logger.info('Attempting to {0} index {1} because it is {2} older than cutoff.'.format(words['op'], index_name, expiration))
        else:
            logger.info('Attempting to {0} index {1} due to space constraints.'.format(words['op'].lower(), index_name))

        skipped = op(client, index_name, **kwargs)

        if skipped:
            continue

        # if no error was raised and we got here that means the operation succeeded
        logger.info('{0}: Successfully {1}.'.format(index_name, words['verbed']))
    if 'for' in words['op']:
        w = words['op'][:-4]
    else:
        w = words['op']
    logger.info('{0} index operations completed.'.format(w.upper()))

def get_segmentcount(client, index_name):
    """Return a list of shardcount, segmentcount"""
    shards = client.indices.segments(index=index_name)['indices'][index_name]['shards']
    segmentcount = 0
    totalshards = 0 # We will increment this manually to capture all replicas...
    for shardnum in shards:
        for shard in range(0,len(shards[shardnum])):
            segmentcount += shards[shardnum][shard]['num_search_segments']
            totalshards += 1
    return totalshards, segmentcount

def create_repo_body(argdict):
    """Create the request body for creating a repository"""
    body = {}
    body['type'] = argdict['repo_type']
    body['settings'] = {}
    settings = ['compress', 'concurrent_streams']
    maybes   = ['chunk_size', 'max_restore_bytes_per_sec', 'max_snapshot_bytes_per_sec']
    s3      = ['bucket', 'region', 'base_path', 'access_key', 'secret_key']

    settings += [i for i in maybes if argdict[i]]
    # Type 'fs'
    if argdict['repo_type'] == 'fs':
        settings.append('location')
    # Type 's3'
    if argdict['repo_type'] == 's3':
        settings += [i for i in s3]
    for k in settings:
        body['settings'][k] = argdict[k]
    return body

def create_snapshot_body(indices, argdict):
    """Create the request body for creating a snapshot"""
    body = {
        "ignore_unavailable": argdict['ignore_unavailable'],
        "include_global_state": argdict['include_global_state'],
        "partial": argdict['partial'],
    }
    if type(indices) is not type(list()):   # in case of a single value passed
        indices = [indices]
    body["indices"] = ','.join(sorted(indices))
    return body

def main():
    start = time.time()

    parser = make_parser()
    arguments = parser.parse_args()
    argdict = arguments.__dict__

    # Do not log and force dry-run if we opt to show indices.
    if arguments.show_indices or arguments.show_repositories or arguments.show_snapshots:
        arguments.log_file = '/dev/null'
        arguments.dry_run = True

    # Setup logging
    if arguments.debug:
        numeric_log_level = logging.DEBUG
    else:
        numeric_log_level = getattr(logging, arguments.log_level.upper(), None)
        if not isinstance(numeric_log_level, int):
            raise ValueError('Invalid log level: %s' % arguments.log_level)
    
    logging.basicConfig(level=numeric_log_level,
                        format='%(asctime)s.%(msecs)03d %(levelname)-9s %(funcName)22s:%(lineno)-4d %(message)s',
                        datefmt="%Y-%m-%dT%H:%M:%S",
                        stream=open(arguments.log_file, 'a') if arguments.log_file else sys.stderr)
    logging.info("Job starting...")

    # Setting up NullHandler to handle nested elasticsearch.trace Logger instance in elasticsearch python client
    logging.getLogger('elasticsearch.trace').addHandler(NullHandler())

    if arguments.show_indices or arguments.show_repositories or arguments.show_snapshots:
        pass # Skip checking args if we're only showing stuff
    else:
        check_args = validate_args(arguments) # Returns either True or a list of errors
        if not check_args == True:
            logger.error('Malformed arguments: {0}'.format(';'.join(check_args)))
            print('See the help output: {0} --help'.format(sys.argv[0]))
            return
    client = elasticsearch.Elasticsearch(host=arguments.host, port=arguments.port, url_prefix=arguments.url_prefix, timeout=arguments.timeout, use_ssl=arguments.ssl)
    
    version_number = get_version(client)
    logger.debug('Detected Elasticsearch version {0}'.format(".".join(map(str,version_number))))
    if version_number >= version_max or version_number < version_min:
        print('Expected Elasticsearch version range > {0} < {1}'.format(".".join(map(str,version_min)),".".join(map(str,version_max))))
        print('ERROR: Incompatible with version {0} of Elasticsearch.  Exiting.'.format(".".join(map(str,version_number))))
        sys.exit(1)

    # Show indices then exit
    if arguments.show_indices:
        for index_name in get_indices(client, arguments.prefix):
            print('{0}'.format(index_name))
        sys.exit(0)
    # Show repositories then exit
    if arguments.show_repositories:
        if not arguments.repository:
            print("Must specify --repository with this option.")
        else:
            for repository in sorted(_get_repository(client, '_all').keys()):
                print('{0}'.format(repository))
        sys.exit(0)
    # Show snapshots from repository then exit
    if arguments.show_snapshots:
        if not arguments.repository:
            print("Must specify --repository with this option.")
        else:
            for snapshot in get_snaplist(client, arguments.repository, prefix=arguments.prefix):
                print('{0}'.format(snapshot))
        sys.exit(0)
    # Create repository
    if arguments.create_repo and arguments.repository and arguments.dry_run:
        logger.info('Would have attempted creating repository {0}...'.format(arguments.repository))
    elif arguments.create_repo and arguments.repository:
        logger.info('Creating repository {0}...'.format(arguments.repository))
        _create_repository(client, arguments.repository, create_repo_body(argdict))
    # Delete repository
    if arguments.delete_repo and arguments.repository and arguments.dry_run:
        logger.info('Would have attempted deleting repository {0}...'.format(arguments.repository))
    elif arguments.delete_repo and arguments.repository:
        logger.info('Deleting repository {0}...'.format(arguments.repository))
        _delete_repository(client, arguments.repository)
    # Delete by space first
    if arguments.disk_space:
        logger.info('Deleting indices by disk usage over {0} gigabytes'.format(arguments.disk_space))
        expired_indices = find_overusage_indices(client, arguments.disk_space, arguments.separator, arguments.prefix)
        index_loop(client, 'delete', expired_indices, arguments.dry_run, by_space=True)
    # Delete by time
    if arguments.delete_older:
        logger.info('Deleting indices older than {0} {1}...'.format(arguments.delete_older, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.delete_older, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'delete', expired_indices, arguments.dry_run)
    # Close by time
    if arguments.close_older:
        logger.info('Closing indices older than {0} {1}...'.format(arguments.close_older, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.close_older, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'close', expired_indices, arguments.dry_run)
    # Disable bloom filter by time
    if arguments.bloom_older:
        logger.info('Disabling bloom filter on indices older than {0} {1}...'.format(arguments.bloom_older, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.bloom_older, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'bloom', expired_indices, arguments.dry_run)
    # Optimize index
    if arguments.optimize:
        logger.info('Optimizing indices older than {0} {1}...'.format(arguments.optimize, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.optimize, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'optimize', expired_indices, arguments.dry_run, max_num_segments=arguments.max_num_segments)
    # Required routing rules
    if arguments.require:
        logger.info('Updating required routing allocation rules on indices older than {0} {1}...'.format(arguments.require, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.require, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'require', expired_indices, arguments.dry_run, attr=arguments.required_rule)
    # Take snapshot
    if arguments.snap_older and arguments.repository:
        logger.info('Adding snapshot of indices older than {0} {1} to repository {2}...'.format(arguments.snap_older, arguments.time_unit, arguments.repository))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.snap_older, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'snapshot', expired_indices, arguments.dry_run, prefix=arguments.prefix, argdict=argdict)
    # Delete snapshot
    if arguments.delete_snaps and arguments.repository:
        logger.info('Deleting snapshots older than {0} {1}...'.format(arguments.delete_snaps, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.delete_snaps, data_type='snapshot', repo_name=arguments.repository, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'delete_snaps', expired_indices, arguments.dry_run, prefix=arguments.prefix, repo_name=arguments.repository)
    if arguments.snap_latest and arguments.repository:
        logger.info('Adding snapshot of {0} most recent indices matching prefix \'{1}\' to repository {2}...'.format(arguments.snap_latest, arguments.prefix, arguments.repository))
        snap_latest_indices(client, arguments.snap_latest, prefix=arguments.prefix, dry_run=arguments.dry_run, argdict=argdict)

    logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))

if __name__ == '__main__':
    main()
