#!/usr/bin/env python

import os
import sys
import time
import logging
import re
from datetime import timedelta, datetime, date

import elasticsearch

try:
    from logging import NullHandler
except ImportError:
    from logging import Handler

    class NullHandler(Handler):
        def emit(self, record):
            pass

__version__ = '1.2.2'

# Elasticsearch versions supported
version_max  = (2, 0, 0)
version_min = (1, 0, 0)
        
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'host': 'localhost',
    'url_prefix': '',
    'port': 9200,
    'auth': None,
    'ssl': False,
    'timeout': 30,
    'prefix': 'logstash-',
    'curation_style': 'time',
    'time_unit': 'days',
    'max_num_segments': 2,
    'dry_run': False,
    'debug': False,
    'log_level': 'INFO',
    'logformat': 'Default',
    'show_indices': False,
    'wait_for_completion': True,
    'ignore_unavailable': False,
    'include_global_state': False,
    'partial': False,
}

datemap = {
    'months': '%Y.%m',
    'weeks': '%Y.%W',
    'days': '%Y.%m.%d',
    'hours': '%Y.%m.%d.%H',
}

def add_common_args(subparser):
    """Add common arguments here to reduce redundancy and line count"""
    subparser.add_argument('-p', '--prefix', help='Prefix for the indices. Indices that do not have this prefix are skipped. Default: logstash-', default=DEFAULT_ARGS['prefix'])
    subparser.add_argument('--timestring', help="Python strftime string to match your index definition, e.g. 2014.07.15 would be %%Y.%%m.%%d", type=str, default=None)
    subparser.add_argument('-T', '--time-unit', dest='time_unit', action='store', help='Unit of time to reckon by: [hours|days|weeks|months] Default: days',
                        default=DEFAULT_ARGS['time_unit'], type=str)
    subparser.add_argument('--exclude-pattern', help='Exclude indices matching provided pattern, e.g. 2014.06.08', type=str, default=None)
    
def make_parser():
    """ Creates an ArgumentParser to parse the command line options. """
    help_desc = 'Curator for Elasticsearch indices. See http://github.com/elasticsearch/curator/wiki'
    try:
        import argparse
        parser = argparse.ArgumentParser(description=help_desc)
        parser.add_argument('-v', '--version', action='version', version='%(prog)s '+__version__)
    except ImportError:
        print('{0} requires module argparse.  Try: pip install argparse'.format(sys.argv[0]))
        sys.exit(1)

    # Common args
    parser.add_argument('--host', help='Elasticsearch host. Default: localhost', default=DEFAULT_ARGS['host'])
    parser.add_argument('--url_prefix', help='Elasticsearch http url prefix. Default: none', default=DEFAULT_ARGS['url_prefix'])
    parser.add_argument('--port', help='Elasticsearch port. Default: 9200', default=DEFAULT_ARGS['port'], type=int)
    parser.add_argument('--ssl', help='Connect to Elasticsearch through SSL. Default: false', action='store_true', default=DEFAULT_ARGS['ssl'])
    parser.add_argument('--auth', help='Use Basic Authentication ex: user:pass Default: None', default=DEFAULT_ARGS['auth'])
    parser.add_argument('-t', '--timeout', help='Connection timeout in seconds. Default: 30', default=DEFAULT_ARGS['timeout'], type=int)
    parser.add_argument('--master-only', dest='master_only', action='store_true', help='Verify that the node is the elected master before continuing', default=False)
    parser.add_argument('-n', '--dry-run', action='store_true', help='If true, does not perform any changes to the Elasticsearch indices.', default=DEFAULT_ARGS['dry_run'])
    parser.add_argument('-D', '--debug', dest='debug', action='store_true', help='Debug mode', default=DEFAULT_ARGS['debug'])
    parser.add_argument('--loglevel', dest='log_level', action='store', help='Log level', default=DEFAULT_ARGS['log_level'], type=str)
    parser.add_argument('-l', '--logfile', dest='log_file', help='log file', type=str)
    parser.add_argument('--logformat', dest='logformat', help='Log output format [default|logstash]. Default: default', default=DEFAULT_ARGS['logformat'], type=str)

    # Command sub_parsers
    subparsers = parser.add_subparsers(
            title='Commands', dest='command', description='Select one of the following commands:',
            help='Run: ' + sys.argv[0] + ' COMMAND --help for command-specific help.')

    # Alias
    parser_alias = subparsers.add_parser('alias', help='Aliasing operations')
    parser_alias.set_defaults(func=alias_loop)
    add_common_args(parser_alias)
    parser_alias.add_argument('--alias', required=True, help='Alias name', type=str)
    alias_group = parser_alias.add_mutually_exclusive_group()
    alias_group.add_argument('--alias-older-than', help='Add indices older than n TIME_UNITs to alias', type=int)
    alias_group.add_argument('--unalias-older-than', help='Remove indices older than n TIME_UNITs from alias', type=int)

    # Allocation
    parser_allocation = subparsers.add_parser('allocation', help='Apply required index routing allocation rule')
    parser_allocation.set_defaults(func=command_loop)
    add_common_args(parser_allocation)
    parser_allocation.add_argument('--older-than', required=True, help='Apply rule to indices older than n TIME_UNITs', type=int)
    parser_allocation.add_argument('--rule', required=True, help='Routing allocation rule to apply, e.g. tag=ssd', type=str)

    # Bloom
    parser_bloom = subparsers.add_parser('bloom', help='Disable bloom filter cache for indices')
    parser_bloom.set_defaults(func=command_loop)
    add_common_args(parser_bloom)
    parser_bloom.add_argument('--older-than', required=True, help='Disable bloom filter cache for indices older than n TIME_UNITs', type=int)

    # Close
    parser_close = subparsers.add_parser('close', help='Close indices')
    parser_close.set_defaults(func=command_loop)
    add_common_args(parser_close)
    parser_close.add_argument('--older-than', required=True, help='Close indices older than n TIME_UNITs', type=int)

    # Delete
    parser_delete = subparsers.add_parser('delete', help='Delete indices')
    parser_delete.set_defaults(func=command_loop)
    add_common_args(parser_delete)
    delete_group = parser_delete.add_mutually_exclusive_group()
    delete_group.add_argument('--older-than', help='Delete indices older than n TIME_UNITs', type=int)
    delete_group.add_argument('--disk-space', help='Delete indices beyond DISK_SPACE gigabytes.', type=float)

    # Optimize
    parser_optimize = subparsers.add_parser('optimize', help='Optimize indices')
    parser_optimize.set_defaults(func=command_loop)
    add_common_args(parser_optimize)
    parser_optimize.add_argument('--older-than', required=True, help='Optimize indices older than n TIME_UNITs', type=int)
    parser_optimize.add_argument('--max_num_segments', help='Optimize segment count to n segments per shard.', default=DEFAULT_ARGS['max_num_segments'], type=int)

    # Show indices
    parser_show = subparsers.add_parser('show', help='Show indices or snapshots')
    parser_show.set_defaults(func=show)
    parser_show.add_argument('-p', '--prefix', help='Prefix for the indices. Indices that do not have this prefix are skipped. Default: logstash-', default=DEFAULT_ARGS['prefix'])
    parser_show.add_argument('--repository', type=str, help='Repository name (required for --show-repositories)')
    show_group = parser_show.add_mutually_exclusive_group()
    show_group.add_argument('--show-indices', help='Show indices matching PREFIX', action='store_true')
    show_group.add_argument('--show-snapshots', help='Show snapshots in REPOSITORY', action='store_true')
    parser_show.add_argument('--exclude-pattern', help='Exclude indices matching provided pattern, e.g. 2014.06.08', type=str, default=None)

    # Snapshot
    parser_snapshot = subparsers.add_parser('snapshot', help='Take snapshots of indices (Backup)')
    parser_snapshot.set_defaults(func=command_loop)
    add_common_args(parser_snapshot)
    parser_snapshot.add_argument('--repository', required=True, type=str, help='Repository name')

    snapshot_group = parser_snapshot.add_mutually_exclusive_group()
    snapshot_group.add_argument('--older-than', type=int, help='Capture snapshots for indices older than n TIME_UNITs.')
    snapshot_group.add_argument('--most-recent', type=int, help='Capture snapshots for n most recent number of indices.')
    snapshot_group.add_argument('--delete-older-than', type=int, help='Delete snapshots older than n TIME_UNITs.')

    parser_snapshot.add_argument('--no_wait_for_completion', action='store_false',
                                help='Do not wait until complete to return. Waits by default.', default=DEFAULT_ARGS['wait_for_completion'])
    parser_snapshot.add_argument('--ignore_unavailable', action='store_true',
                                help='Ignore unavailable shards/indices. Default=False', default=DEFAULT_ARGS['ignore_unavailable'])
    parser_snapshot.add_argument('--include_global_state', action='store_true',
                                help='Store cluster global state with snapshot. Default=False', default=DEFAULT_ARGS['include_global_state'])
    parser_snapshot.add_argument('--partial', action='store_true',
                                help='Do not fail if primary shard is unavailable. Default=False', default=DEFAULT_ARGS['partial'])

    return parser

class Whitelist(logging.Filter):
    def __init__(self, *whitelist):
        self.whitelist = [logging.Filter(name) for name in whitelist]

    def filter(self, record):
        return any(f.filter(record) for f in self.whitelist)

def show(client, **kwargs):
    if kwargs['show_indices']:
        for index_name in get_indices(client, kwargs['prefix']):
            print('{0}'.format(index_name))
        sys.exit(0)
    elif kwargs['show_snapshots']:
        for snapshot in get_snaplist(client, kwargs['repository'], prefix=kwargs['prefix']):
            print('{0}'.format(snapshot))
        sys.exit(0)

def get_index_time(index_timestamp, timestring):
    """ Gets the time of the index.

    :param index_timestamp: A string of the format timestring
    :return The creation time (datetime) of the index.
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
    return datetime.strptime(index_timestamp, timestring)

def find_target_month(month_count, utc_now=None):
    """Return datetime object for # of _full_ months older than month_count from now"""
    utc_now = date(utc_now.year, utc_now.month, 1) if utc_now else date.today()
    target_date = date(utc_now.year, utc_now.month, 1)

    for i in range(0, month_count):
        if target_date.month == 1:
            target_date = date(target_date.year-1, 12, 1)
        else:
            target_date = date(target_date.year, target_date.month-1, 1)

    return datetime(target_date.year, target_date.month, target_date.day)

def get_indices(client, prefix='logstash-', exclude_pattern=None):
    """Return a sorted list of indices matching prefix"""
    _indices = sorted(client.indices.get_settings(index=prefix+'*', params={'expand_wildcards': 'closed'}).keys())
    if exclude_pattern:
        pattern = re.compile(exclude_pattern)
        return list(filter(lambda x: not pattern.search(x), _indices))
    else:
        return _indices
    
def get_snaplist(client, repo_name, prefix='logstash-'):
    """Get _all snapshots containing prefix from repo_name and return a list"""
    retval = []
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        retval = [snap['snapshot'] for snap in allsnaps if 'snapshot' in snap.keys()]
        retval = [i for i in retval if prefix in i]
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
    return retval

def get_snapped_indices(client, repo_name, prefix='logstash-'):
    """Return all indices in snapshots which succeeded and match prefix"""
    from itertools import chain
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        allindices = chain.from_iterable(s['indices'] for s in allsnaps if s['state'] == 'SUCCESS')
        return set(i for i in allindices if i.startswith(prefix))
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
        return []

def get_version(client):
    """Return ES version number as a tuple"""
    version = client.info()['version']['number']
    return tuple(map(int, version.split('.')))

def check_version(client):
    """Verify version is within acceptable range"""
    version_number = get_version(client)
    logger.debug('Detected Elasticsearch version {0}'.format(".".join(map(str,version_number))))
    if version_number >= version_max or version_number < version_min:
        print('Expected Elasticsearch version range > {0} < {1}'.format(".".join(map(str,version_min)),".".join(map(str,version_max))))
        print('ERROR: Incompatible with version {0} of Elasticsearch.  Exiting.'.format(".".join(map(str,version_number))))
        sys.exit(1)

def is_master_node(client):
    my_node_id = client.nodes.info('_local')['nodes'].keys()[0]
    master_node_id = client.cluster.state(metric='master_node')['master_node']
    return my_node_id == master_node_id

def get_object_list(client, data_type='index', prefix='logstash-', repository=None, exclude_pattern=None, **kwargs):
    """Return a list of indices or snapshots"""
    if data_type == 'index':
        object_list = get_indices(client, prefix)
        if '.marvel-kibana' in object_list:
            object_list.remove('.marvel-kibana')
    elif data_type == 'snapshot':
        if repository:
            object_list = get_snaplist(client, repository, prefix=prefix)
        else:
            logger.error('Repository name not specified. Returning empty list.')
            object_list = []
    else:
        object_list = []
        logger.error('data_type \'{0}\' is neither \'index\' nor \'snapshot\'.  Returning empty list.'.format(data_type))
    if exclude_pattern:
        pattern = re.compile(exclude_pattern)
        return list(filter(lambda x: not pattern.search(x), object_list))
    else:
        return object_list
    
def find_expired_data(object_list=[], utc_now=None, time_unit='days', older_than=999999, prefix='logstash-', timestring=None, **kwargs):
    """ Generator that yields expired objects (indices or snapshots).
    
    :return: Yields a list of indices older than n `time_unit`s
    """
    # time-injection for test purposes only
    utc_now = utc_now if utc_now else datetime.utcnow()
    # reset to start of the period to be sure we are not retiring a human by mistake
    utc_now = utc_now.replace(minute=0, second=0, microsecond=0)
    
    if time_unit == 'days':
        utc_now = utc_now.replace(hour=0)
    if time_unit == 'weeks':
        # Since week math always uses Monday as the start of the week,
        # this work-around resets utc_now to be Monday of the current week.
        weeknow = utc_now.strftime('%Y-%W')
        utc_now = get_index_time(weeknow, '%Y-%W')
    if time_unit == 'months':
        utc_now = utc_now.replace(hour=0)
        cutoff = find_target_month(older_than, utc_now=utc_now)
    else:
        # This cutoff must be a multiple of time_units
        cutoff = utc_now - timedelta(**{time_unit: (older_than - 1)})
    
    for object_name in object_list:
    
        index_timestamp = object_name[len(prefix):]

        try:
            object_time = get_index_time(index_timestamp, timestring)
        except ValueError:
            logger.error('Could not find a valid timestamp for {0} with timestring {1}'.format(object_name, timestring))
            continue
    
        # if the index is older than the cutoff
        if object_time < cutoff:
            yield object_name
        else:
            logger.info('{0} is within the threshold period ({1} {2}).'.format(object_name, older_than, time_unit))

def find_overusage_indices(client, disk_space=2097152.0, prefix='logstash-', **kwargs):
    """ Generator that yields over usage indices.

    :return: Yields a list of indices to delete based on space consumed, starting with the oldest.
    """

    disk_usage = 0.0
    disk_limit = disk_space * 2**30

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

def create_snapshot_body(indices, ignore_unavailable=False, include_global_state=False, partial=False, **kwargs):
    """Create the request body for creating a snapshot"""
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if type(indices) is not type(list()):   # in case of a single value passed
        indices = [indices]
    body["indices"] = ','.join(sorted(indices))
    return body
    
def get_repository(client, repo_name):
    """Get Repository information"""
    try:
        return client.snapshot.get_repository(repository=repo_name)
    except elasticsearch.NotFoundError as e:
        logger.info("Repository {0} not found.  Error: {1}".format(repo_name, e))
        return None

def get_snapshot(client, repo_name, snap_name):
    """Get information about a snapshot (or snapshots)"""
    try:
        return client.snapshot.get(repository=repo_name, snapshot=snap_name)
    except elasticsearch.NotFoundError as e:
        logger.info("Snapshot or repository {0} not found.  Error: {1}".format(snap_name, e))
        return None

def _create_snapshot(client, snap_name, prefix='logstash-', repository=None, ignore_unavailable=False, include_global_state=False, partial=False, wait_for_completion=True, **kwargs):
    """Create a snapshot (or snapshots). Overwrite failures"""
    # Return True when it was skipped
    if not repository:
        logger.error("Unable to create snapshot. Repository name not provided.")
        return True
    try:
        successes = get_snapped_indices(client, repository, prefix=prefix)
        snaps = get_snaplist(client, repository, prefix=prefix)
        closed = index_closed(client, snap_name)
        body=create_snapshot_body(snap_name, ignore_unavailable=ignore_unavailable, include_global_state=include_global_state, partial=partial)
        if not snap_name in snaps and not snap_name in successes and not closed:
            client.snapshot.create(repository=repository, snapshot=snap_name, body=body, wait_for_completion=wait_for_completion)
        elif snap_name in snaps and not snap_name in successes and not closed:
            logger.warn("Previous snapshot was unsuccessful.  Deleting snapshot {0} and trying again.".format(snap_name))
            _delete_snapshot(client, repository, snap_name)
            client.snapshot.create(repository=repository, snapshot=snap_name, body=body, wait_for_completion=wait_for_completion)
        elif closed:
            logger.info("Skipping: Index {0} is closed.".format(snap_name))
            return True
        else:
            logger.info("Skipping: A snapshot with name '{0}' already exists.".format(snap_name))
            return True
    except elasticsearch.RequestError as e:
        logger.error("Unable to create snapshot {0}.  Error: {1} Check logs for more information.".format(snap_name, e))
        return True

def _delete_snapshot(client, snap_name, **kwargs):
    """Delete a snapshot (or snapshots)"""
    # kwargs is here to preserve expected number of args passed by command_loop
    client.snapshot.delete(repository=kwargs['repository'], snapshot=snap_name)
    
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
        
def _require_index(client, index_name, **kwargs):
    rule = kwargs['rule']
    key = rule.split('=')[0]
    value = rule.split('=')[1]
    if index_closed(client, index_name):
      logger.info('Skipping index {0}: Already closed.'.format(index_name))
      return True
    else:
      logger.info('Updating index setting index.routing.allocation.require.{0}={1}'.format(key,value))
      client.indices.put_settings(index=index_name, body='index.routing.allocation.require.{0}={1}'.format(key,value))

def get_alias(client, alias):
    if client.indices.exists_alias(alias):
        return client.indices.get_alias(name=alias).keys()
    else:
        logger.error('Unable to find alias {0}.'.format(alias))
        return False

def _remove_from_alias(client, index_name, alias=None, **kwargs):
    indices_in_alias = get_alias(client, alias)
    if not indices_in_alias:
        return True
    if index_name in indices_in_alias:
        client.indices.update_aliases(body={'actions': [{ 'remove': { 'index': index_name, 'alias': alias}}]})
    else:
        logger.info('Index {0} does not exist in alias {1}; skipping.'.format(index_name, alias))
        return True

def _add_to_alias(client, index_name, alias=None, **kwargs):
    if not alias: # This prevents _all from being aliased by accident...
        logger.error('No alias provided.')
        return True
    if not client.indices.exists_alias(alias):
        logger.error('Skipping index {0}: Alias {1} does not exist.'.format(index_name, alias))
        return True
    else:
        indices_in_alias = client.indices.get_alias(alias)
        if not index_name in indices_in_alias:
            client.indices.update_aliases(body={'actions': [{ 'add': { 'index': index_name, 'alias': alias}}]})
        else:
            logger.info('Skipping index {0}: Index already exists in alias {1}...'.format(index_name, alias))
            return True
    
OP_MAP = {
    'allocation'  : (_require_index, {'op': 'update require allocation rules for', 'verbed':'index routing allocation updated', 'gerund': 'Updating required index routing allocation rules for'}),
    'bloom'       : (_bloom_index, {'op': 'disable bloom filter for', 'verbed': 'bloom filter disabled', 'gerund': 'Disabling bloom filter for'}),
    'close'       : (_close_index, {'op': 'close', 'verbed': 'closed', 'gerund': 'Closing'}),
    'delete'      : (_delete_index, {'op': 'delete', 'verbed': 'deleted', 'gerund': 'Deleting'}),
    'optimize'    : (_optimize_index, {'op': 'optimize', 'verbed': 'optimized', 'gerund': 'Optimizing'}),
    'snapshot'    : (_create_snapshot, {'op': 'create snapshot for', 'verbed':'created snapshot', 'gerund': 'Initiating snapshot for'}),
}

def snap_latest_indices(client, most_recent=0, prefix='logstash-', dry_run=False, **kwargs):
    """Snapshot 'count' most recent indices matching prefix"""
    indices = [] # initialize...
    indices = get_indices(client, prefix)
    prepend = "DRY RUN: " if dry_run else ''
    for index_name in indices[-most_recent:]:
        if not index_closed(client, index_name):
            logger.info(prepend + 'Attempting to create snapshot for {0}...'.format(index_name))
        else:
            logger.warn(prepend + 'Unable to perform snapshot on closed index {0}'.format(index_name))
            continue
        if dry_run:
            continue # Don't do the work on a dry run
        
        skipped = _create_snapshot(client, index_name, prefix, **kwargs)
            
        if skipped:
            continue
        # if no error was raised and we got here that means the operation succeeded
        logger.info('Snapshot operation for index {0} succeeded.'.format(index_name))
    logger.info(prepend + 'Snapshot \'latest\' {0} indices operations completed.'.format(most_recent))

def alias_loop(client, dry_run=False, **kwargs):
    prepend = "DRY RUN: " if dry_run else ''
    logging.info(prepend + "Beginning ALIAS operations...")
    if kwargs['alias_older_than']:
        kwargs['older_than'] = kwargs['alias_older_than']
        op = _add_to_alias
        words = ['add', 'to', 'added']
    elif kwargs['unalias_older_than']:
        kwargs['older_than'] = kwargs['unalias_older_than']
        op = _remove_from_alias
        words = ['remove', 'from', 'removed']
    index_list = get_object_list(client, **kwargs)
    expired_indices = find_expired_data(object_list=index_list, **kwargs)
    for index_name in expired_indices:
        logger.info(prepend + 'Attempting to {0} index {1} {2} alias {3}.'.format(words[0], index_name, words[1], kwargs['alias']))
        if dry_run:
            continue

        skipped = op(client, index_name, **kwargs)
        if skipped:
            continue
        # if no error was raised and we got here that means the operation succeeded
        logger.info('{0}: Successfully {1} {2} alias {3}.'.format(index_name, words[2], words[1], kwargs['alias']))
    logger.info(prepend + 'Index ALIAS operations completed.')

def command_loop(client, dry_run=False, **kwargs):
    prepend = "DRY RUN: " if dry_run else ''
    command = kwargs['command']
    logging.info(prepend + "Beginning {0} operations...".format(command.upper()))
    op, words = OP_MAP[command]
    by_space = kwargs['disk_space'] if 'disk_space' in kwargs else False
    if command == 'delete' and by_space:
        expired_indices = find_overusage_indices(client, **kwargs)
    elif command == 'snapshot' and kwargs['delete_older_than']:
        kwargs['older_than'] = kwargs['delete_older_than'] # Fix for delete in this case only.
        snapshot_list = get_object_list(client, data_type='snapshot', **kwargs)
        expired_indices = find_expired_data(object_list=snapshot_list, **kwargs)
        op = _delete_snapshot
        words = {'op': 'delete snapshot for', 'verbed':'deleted snapshot', 'gerund': 'Deleting snapshot for'}
    elif command == 'snapshot' and kwargs['most_recent']:
        snap_latest_indices(client, **kwargs)
        return
    else: # Regular indexes
        index_list = get_object_list(client, **kwargs)
        expired_indices = find_expired_data(object_list=index_list, **kwargs)

    for index_name in expired_indices:
        if not by_space:
            logger.info(prepend + 'Attempting to {0} index {1}.'.format(words['op'], index_name))
        else:
            logger.info(prepend + 'Attempting to {0} index {1} due to space constraints.'.format(words['op'].lower(), index_name))
        if dry_run:
            continue # Don't act on dry run

        skipped = op(client, index_name, **kwargs)

        if skipped:
            continue

        # if no error was raised and we got here that means the operation succeeded
        logger.info('{0}: Successfully {1}.'.format(index_name, words['verbed']))
    if 'for' in words['op']:
        w = words['op'][:-4]
    else:
        w = words['op']
    logger.info(prepend + '{0} index operations completed.'.format(w.upper()))

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

def validate_timestring(timestring, time_unit):
    """
    Validate that the appropriate element(s) for time_unit are in the timestring.
    e.g. If "weeks", we should see %U or %W, if hours %H, etc.
    """
    fail = True
    if time_unit == 'hours':
        if '%H' in timestring:
            fail = False
    elif time_unit == 'days':
        if '%d' in timestring:
            fail = False
    elif time_unit == 'weeks':
        if '%W' in timestring:
            fail = False
        elif '%U' in timestring:
            fail = False
    if fail:
        print('Timestring {0} does not match time unit {1}'.format(timestring, time_unit))
        sys.exit(1)
    return

    
def main():
    start = time.time()

    parser = make_parser()
    arguments = parser.parse_args()

    # Initialize timeout_override
    timeout_override = True if arguments.command == 'optimize' else False

    # Argparse nearly gets all conditions covered.
    # These remain because mutually exclusive arguments must be optional.
    if arguments.command == 'alias':
        if not arguments.alias_older_than and not arguments.unalias_older_than:
            print('{0} delete: error: expect one of --alias-older-than or --unalias-older-than'.format(sys.argv[0]))
            sys.exit(1)

    if arguments.command == 'delete':
        if not arguments.older_than and not arguments.disk_space:
            print('{0} delete: error: expect one of --older-than or --disk-space'.format(sys.argv[0]))
            sys.exit(1)

    if arguments.command == 'show':
        # Do not log and force dry-run if we opt to show indices or snapshots.
        arguments.log_file = os.devnull
        arguments.dry_run = True
        if not arguments.show_indices and not arguments.show_snapshots:
            print('{0} show: error: expect one of --show-indices or --show-snapshots'.format(sys.argv[0]))
            sys.exit(1)
        if arguments.show_snapshots and not arguments.repository:
            print('{0} show: error: --repository required with --show-snapshots'.format(sys.argv[0]))
            sys.exit(1)

    if arguments.command == 'snapshot':
        if not arguments.older_than and not arguments.most_recent and not arguments.delete_older_than:
            print('{0} snapshot: error: expect one of --older-than, --most-recent, or --delete-older-than'.format(sys.argv[0]))
            sys.exit(1)
        if arguments.older_than or arguments.most_recent:
            timeout_override = True

    # Setup logging
    if arguments.debug:
        numeric_log_level = logging.DEBUG
        format_string = '%(asctime)s %(levelname)-9s %(name)22s %(funcName)22s:%(lineno)-4d %(message)s'
    else:
        numeric_log_level = getattr(logging, arguments.log_level.upper(), None)
        format_string = '%(asctime)s %(levelname)-9s %(message)s'
        if not isinstance(numeric_log_level, int):
            raise ValueError('Invalid log level: %s' % arguments.log_level)
    
    date_string = None
    if arguments.logformat == 'logstash':
        os.environ['TZ'] = 'UTC'
        time.tzset()
        format_string = '{"@timestamp":"%(asctime)s.%(msecs)03dZ", "loglevel":"%(levelname)s", "name":"%(name)s", function":"%(funcName)s", "linenum":"%(lineno)d", "message":"%(message)s"}'
        date_string = '%Y-%m-%dT%H:%M:%S'

    logging.basicConfig(level=numeric_log_level,
                        format=format_string,
                        datefmt=date_string,
                        stream=open(arguments.log_file, 'a') if arguments.log_file else sys.stderr)

    # Filter out logging from Elasticsearch and associated modules by default
    if not arguments.debug:
        for handler in logging.root.handlers:
            handler.addFilter(Whitelist('root', '__main__', 'curator.curator'))

    # Setting up NullHandler to handle nested elasticsearch.trace Logger instance in elasticsearch python client
    logging.getLogger('elasticsearch.trace').addHandler(NullHandler())

    logging.info("Job starting...")

    if arguments.dry_run:
        logging.info("DRY RUN MODE.  No changes will be made.")

    # Override the timestamp in case the end-user doesn't.
    if timeout_override and arguments.timeout == 30:
        logger.info('Default timeout of 30 seconds is too low for command {0}.  Overriding to 21,600 seconds (6 hours).'.format(arguments.command.upper()))
        arguments.timeout = 21600

    client = elasticsearch.Elasticsearch(host=arguments.host, http_auth=arguments.auth, port=arguments.port, url_prefix=arguments.url_prefix, timeout=arguments.timeout, use_ssl=arguments.ssl)
    
    # Verify the version is acceptable.
    check_version(client)
    
    if arguments.master_only and not is_master_node(client):
        logger.fatal('Master-only flag detected. Connected to non-master node. Aborting.')
        sys.exit(1)

    if arguments.command != "show":
        if arguments.timestring:
            validate_timestring(arguments.timestring, arguments.time_unit)
        else: # Set default timestrings
            arguments.timestring = datemap[arguments.time_unit]
            logging.debug("Setting default timestring for {0} to {1}".format(arguments.time_unit, arguments.timestring))
        logging.debug("Matching indices with pattern: {0}{1}".format(arguments.prefix,arguments.timestring))

    # Execute the command specified in the arguments
    argdict = arguments.__dict__
    logging.debug("argdict = {0}".format(argdict))
    arguments.func(client, **argdict)

    logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))

if __name__ == '__main__':
    main()
    
