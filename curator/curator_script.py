#!/usr/bin/env python

import os
import sys
import time
import logging
from datetime import timedelta, datetime, date

import elasticsearch
#from curator import *
import curator

try:
    from logging import NullHandler
except ImportError:
    from logging import Handler

    class NullHandler(Handler):
        def emit(self, record):
            pass

__version__ = '2.0.1'

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
    'suffix': '',
    'curation_style': 'time',
    'time_unit': 'days',
    'max_num_segments': 2,
    'dry_run': False,
    'debug': False,
    'log_level': 'INFO',
    'logformat': 'Default',
    'all_indices': False,
    'show_indices': False,
    'snapshot_prefix': 'curator-',
    'wait_for_completion': True,
    'ignore_unavailable': False,
    'include_global_state': True,
    'partial': False,
}

DATEMAP = {
    'months': '%Y.%m',
    'weeks': '%Y.%W',
    'days': '%Y.%m.%d',
    'hours': '%Y.%m.%d.%H',
}

def add_common_args(subparser):
    """Add common arguments here to reduce redundancy and line count"""
    subparser.add_argument('--timestring', help="Python strftime string to match your index definition, e.g. 2014.07.15 would be %%Y.%%m.%%d", type=str, default=None)
    subparser.add_argument('--prefix', help='Define a prefix. Index name = PREFIX + TIMESTRING + SUFFIX. Default: logstash-', default=DEFAULT_ARGS['prefix'])
    subparser.add_argument('--suffix', help='Define a suffix. Index name = PREFIX + TIMESTRING + SUFFIX. Default: Empty', default=DEFAULT_ARGS['suffix'])
    subparser.add_argument('--time-unit', dest='time_unit', action='store', help='Unit of time to reckon by: [hours|days|weeks|months] Default: days',
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
    parser.add_argument('--timeout', help='Connection timeout in seconds. Default: 30', default=DEFAULT_ARGS['timeout'], type=int)
    parser.add_argument('--master-only', dest='master_only', action='store_true', help='Verify that the node is the elected master before continuing', default=False)
    parser.add_argument('-n', '--dry-run', action='store_true', help='If true, does not perform any changes to the Elasticsearch indices.', default=DEFAULT_ARGS['dry_run'])
    parser.add_argument('-D', '--debug', dest='debug', action='store_true', help='Debug mode', default=DEFAULT_ARGS['debug'])
    parser.add_argument('--loglevel', dest='log_level', action='store', help='Log level', default=DEFAULT_ARGS['log_level'], type=str)
    parser.add_argument('--logfile', dest='log_file', help='log file', type=str)
    parser.add_argument('--logformat', dest='logformat', help='Log output format [default|logstash]. Default: default', default=DEFAULT_ARGS['logformat'], type=str)

    # Command sub_parsers
    subparsers = parser.add_subparsers(
            title='Commands', dest='command', description='Select one of the following commands:',
            help='Run: ' + sys.argv[0] + ' COMMAND --help for command-specific help.')

    # Alias
    parser_alias = subparsers.add_parser('alias', help='Aliasing operations')
    parser_alias.set_defaults(func=curator.alias)
    add_common_args(parser_alias)
    parser_alias.add_argument('--alias', required=True, help='Alias name', type=str)
    alias_group = parser_alias.add_mutually_exclusive_group()
    alias_group.add_argument('--alias-older-than', help='Add indices older than n TIME_UNITs to alias', type=int)
    alias_group.add_argument('--unalias-older-than', help='Remove indices older than n TIME_UNITs from alias', type=int)

    # Allocation
    parser_allocation = subparsers.add_parser('allocation', help='Apply required index routing allocation rule')
    parser_allocation.set_defaults(func=curator.allocation)
    add_common_args(parser_allocation)
    parser_allocation.add_argument('--older-than', required=True, help='Apply rule to indices older than n TIME_UNITs', type=int)
    parser_allocation.add_argument('--rule', required=True, help='Routing allocation rule to apply, e.g. tag=ssd', type=str)

    # Bloom
    parser_bloom = subparsers.add_parser('bloom', help='Disable bloom filter cache for indices')
    parser_bloom.set_defaults(func=curator.bloom)
    add_common_args(parser_bloom)
    parser_bloom.add_argument('--older-than', required=True, help='Disable bloom filter cache for indices older than n TIME_UNITs', type=int)

    # Close
    parser_close = subparsers.add_parser('close', help='Close indices')
    parser_close.set_defaults(func=curator.close)
    add_common_args(parser_close)
    parser_close.add_argument('--older-than', required=True, help='Close indices older than n TIME_UNITs', type=int)

    # Delete
    parser_delete = subparsers.add_parser('delete', help='Delete indices')
    parser_delete.set_defaults(func=curator.delete)
    add_common_args(parser_delete)
    delete_group = parser_delete.add_mutually_exclusive_group()
    delete_group.add_argument('--older-than', help='Delete indices older than n TIME_UNITs', type=int)
    delete_group.add_argument('--disk-space', help='Delete indices beyond DISK_SPACE gigabytes.', type=float)

    # Optimize
    parser_optimize = subparsers.add_parser('optimize', help='Optimize indices')
    parser_optimize.set_defaults(func=curator.optimize)
    add_common_args(parser_optimize)
    parser_optimize.add_argument('--older-than', required=True, help='Optimize indices older than n TIME_UNITs', type=int)
    parser_optimize.add_argument('--max_num_segments', help='Optimize segment count to n segments per shard.', default=DEFAULT_ARGS['max_num_segments'], type=int)
    parser_optimize.add_argument('--delay', help='Number of seconds to delay after optimizing an index.', type=int, default=0)

    # Show indices
    parser_show = subparsers.add_parser('show', help='Show indices or snapshots')
    parser_show.set_defaults(func=show)
    parser_show.add_argument('--prefix', help='Define a prefix. Index name = PREFIX + TIMESTRING + SUFFIX. Default: logstash-', default=DEFAULT_ARGS['prefix'])
    parser_show.add_argument('--suffix', help='Define a suffix. Index name = PREFIX + TIMESTRING + SUFFIX. Default: None', default='')
    parser_show.add_argument('--repository', type=str, help='Repository name (required for --show-repositories)')
    show_group = parser_show.add_mutually_exclusive_group()
    show_group.add_argument('--show-indices', help='Show indices matching PREFIX', action='store_true')
    show_group.add_argument('--show-snapshots', help='Show snapshots in REPOSITORY', action='store_true')
    parser_show.add_argument('--snapshot-prefix', type=str, help='Override default name.', default=DEFAULT_ARGS['snapshot_prefix'])
    parser_show.add_argument('--exclude-pattern', help='Exclude indices matching provided pattern, e.g. 2014.06.08', type=str, default=None)
    

    # Snapshot
    parser_snapshot = subparsers.add_parser('snapshot', help='Take snapshots of indices (Backup)')
    parser_snapshot.set_defaults(func=curator.snapshot)
    add_common_args(parser_snapshot)
    parser_snapshot.add_argument('--repository', required=True, type=str, help='Repository name')
    parser_snapshot.add_argument('--snapshot-name', type=str, help='Override default name.')
    parser_snapshot.add_argument('--snapshot-prefix', type=str, help='Override default name.', default=DEFAULT_ARGS['snapshot_prefix'])

    snapshot_group = parser_snapshot.add_mutually_exclusive_group()
    snapshot_group.add_argument('--older-than', type=int, help='Capture snapshots for indices older than n TIME_UNITs.')
    snapshot_group.add_argument('--all-indices', action='store_true', help='Capture "_all" indices (Elasticsearch default).', default=DEFAULT_ARGS['all_indices'])
    snapshot_group.add_argument('--most-recent', type=int, help='Capture snapshots for n most recent number of indices.')
    snapshot_group.add_argument('--delete-older-than', type=int, help='Delete snapshots older than n TIME_UNITs.')

    parser_snapshot.add_argument('--no_wait_for_completion', action='store_false',
                                help='Do not wait until complete to return. Waits by default.', default=DEFAULT_ARGS['wait_for_completion'])
    parser_snapshot.add_argument('--ignore_unavailable', action='store_true',
                                help='Ignore unavailable shards/indices. Default=False', default=DEFAULT_ARGS['ignore_unavailable'])
    parser_snapshot.add_argument('--include_global_state', action='store_false',
                                help='Store cluster global state with snapshot. Default=True', default=DEFAULT_ARGS['include_global_state'])
    parser_snapshot.add_argument('--partial', action='store_true',
                                help='Do not fail if primary shard is unavailable. Default=False', default=DEFAULT_ARGS['partial'])

    return parser

class Whitelist(logging.Filter):
    def __init__(self, *whitelist):
        self.whitelist = [logging.Filter(name) for name in whitelist]

    def filter(self, record):
        return any(f.filter(record) for f in self.whitelist)

def show(client, **kwargs):
    """
    Show indices or snapshots matching supplied parameters and exit.
    
    :arg client: The Elasticsearch client connection
    :arg data_type: Either ``index`` or ``snapshot``
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg repository: The Elasticsearch snapshot repository to use (only with
        snapshots)
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    """
    if kwargs['show_indices']:
        for index_name in curator.get_indices(client, prefix=kwargs['prefix'], suffix=kwargs['suffix']):
            print('{0}'.format(index_name))
        sys.exit(0)
    elif kwargs['show_snapshots']:
        for snapshot in curator.get_snaplist(client, kwargs['repository'], snapshot_prefix=kwargs['snapshot_prefix']):
            print('{0}'.format(snapshot))
        sys.exit(0)

def check_version(client):
    """
    Verify version is within acceptable range.  Exit with error if it is not.
    
    :arg client: The Elasticsearch client connection
    """
    version_number = curator.get_version(client)
    logger.debug('Detected Elasticsearch version {0}'.format(".".join(map(str,version_number))))
    if version_number >= version_max or version_number < version_min:
        print('Expected Elasticsearch version range > {0} < {1}'.format(".".join(map(str,version_min)),".".join(map(str,version_max))))
        print('ERROR: Incompatible with version {0} of Elasticsearch.  Exiting.'.format(".".join(map(str,version_number))))
        sys.exit(1)

def validate_timestring(timestring, time_unit):
    """
    Validate that the appropriate element(s) for time_unit are in the timestring.
    e.g. If "weeks", we should see %U or %W, if hours %H, etc.
    
    Exit with error on failure.
    
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
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
    elif time_unit == 'months':
        if '%m' in timestring:
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
        if not arguments.older_than and not arguments.most_recent and not arguments.delete_older_than and not arguments.all_indices:
            print('{0} snapshot: error: expect one of --all-indices, --older-than, --most-recent, or --delete-older-than'.format(sys.argv[0]))
            sys.exit(1)
        if arguments.older_than or arguments.most_recent or arguments.all_indices:
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
        format_string = '{"@timestamp":"%(asctime)s.%(msecs)03dZ", "loglevel":"%(levelname)s", "name":"%(name)s", "function":"%(funcName)s", "linenum":"%(lineno)d", "message":"%(message)s"}'
        date_string = '%Y-%m-%dT%H:%M:%S'

    logging.basicConfig(level=numeric_log_level,
                        format=format_string,
                        datefmt=date_string,
                        stream=open(arguments.log_file, 'a') if arguments.log_file else sys.stderr)

    # Filter out logging from Elasticsearch and associated modules by default
    if not arguments.debug:
        for handler in logging.root.handlers:
            handler.addFilter(Whitelist('root', '__main__', 'curator', 'curator.curator'))

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
    
    if arguments.master_only and not curator.is_master_node(client):
        logger.info('Master-only flag detected. Connected to non-master node. Aborting.')
        sys.exit(0)

    if arguments.command != "show":
        if arguments.timestring:
            validate_timestring(arguments.timestring, arguments.time_unit)
        else: # Set default timestrings
            arguments.timestring = DATEMAP[arguments.time_unit]
            logging.debug("Setting default timestring for {0} to {1}".format(arguments.time_unit, arguments.timestring))
        logging.debug("Matching indices with pattern: {0}{1}".format(arguments.prefix,arguments.timestring))

    # Execute the command specified in the arguments
    argdict = arguments.__dict__
    logging.debug("argdict = {0}".format(argdict))
    arguments.func(client, **argdict)

    logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))

if __name__ == '__main__':
    main()
    
