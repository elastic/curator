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
import re
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

__version__ = '1.0.1-dev'

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
    'show_indices': False
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
    parser.add_argument('-t', '--timeout', help='Elasticsearch timeout. Default: 30', default=DEFAULT_ARGS['timeout'], type=int)

    parser.add_argument('-p', '--prefix', help='Prefix for the indices. Indices that do not have this prefix are skipped. Default: logstash-', default=DEFAULT_ARGS['prefix'])
    parser.add_argument('--exclude-pattern', help='Indices matching this re pattern are skipped. Combine with -p option')
    parser.add_argument('-s', '--separator', help='Time unit separator. Default: .', default=DEFAULT_ARGS['separator'])

    parser.add_argument('-C', '--curation-style', dest='curation_style', action='store', help='Curate indices by [time, space] Default: time', default=DEFAULT_ARGS['curation_style'], type=str)
    parser.add_argument('-T', '--time-unit', dest='time_unit', action='store', help='Unit of time to reckon by: [days, hours] Default: days', default=DEFAULT_ARGS['time_unit'], type=str)

    parser.add_argument('-d', '--delete', dest='delete_older', action='store', help='Delete indices older than n TIME_UNITs.', type=int)
    parser.add_argument('-c', '--close', dest='close_older', action='store', help='Close indices older than n TIME_UNITs.', type=int)
    parser.add_argument('-b', '--bloom', dest='bloom_older', action='store', help='Disable bloom filter for indices older than n TIME_UNITs.', type=int)
    parser.add_argument('-g', '--disk-space', dest='disk_space', action='store', help='Delete indices beyond n GIGABYTES.', type=float)
    
    parser.add_argument('-r', '--require', help='Update indices required routing allocation rules. Ex. tag=ssd', type=int)
    parser.add_argument('--required_rule', help='Index routing allocation rule to require. Ex. tag=ssd', type=str)
    
    parser.add_argument('--max_num_segments', action='store', help='Maximum number of segments, post-optimize. Default: 2', type=int, default=DEFAULT_ARGS['max_num_segments'])
    parser.add_argument('-o', '--optimize', action='store', help='Optimize (Lucene forceMerge) indices older than n TIME_UNITs.  Must increase timeout to stay connected throughout optimize operation, recommend no less than 3600.', type=int)

    parser.add_argument('-n', '--dry-run', action='store_true', help='If true, does not perform any changes to the Elasticsearch indices.', default=DEFAULT_ARGS['dry_run'])
    parser.add_argument('-D', '--debug', dest='debug', action='store_true', help='Debug mode', default=DEFAULT_ARGS['debug'])
    parser.add_argument('-ll', '--loglevel', dest='log_level', action='store', help='Log level', default=DEFAULT_ARGS['log_level'], type=str)
    parser.add_argument('-l', '--logfile', dest='log_file', help='log file', type=str)
    parser.add_argument('--show-indices', dest='show_indices', action='store_true', help='Show indices matching prefix', default=DEFAULT_ARGS['show_indices'])

    return parser


def validate_args(myargs):
    """Validate that arguments aren't stomping on each other or conflicting"""
    success = True
    messages = []
    if myargs.curation_style == 'time':
        if not myargs.delete_older and not myargs.close_older and not myargs.bloom_older and not myargs.optimize and not myargs.require:
            success = False
            messages.append('Must specify at least one of --delete, --close, --bloom, --optimize or --require')
        if ((myargs.delete_older and myargs.delete_older < 1) or
            (myargs.close_older and myargs.close_older < 1) or
            (myargs.bloom_older and myargs.bloom_older < 1) or
            (myargs.optimize and myargs.optimize < 1)):
            success = False
            messages.append('Values for --delete, --close, --bloom or --optimize must be > 0')
        if myargs.time_unit != 'days' and myargs.time_unit != 'hours':
            success = False
            messages.append('Values for --time-unit must be either "days" or "hours"')
        if myargs.disk_space:
            success = False
            messages.append('Cannot specify --disk-space and --curation-style "time"')
        if myargs.optimize and myargs.timeout < 300:
            success = False
            messages.append('Timeout should be much higher for optimize transactions, recommend no less than 3600 seconds')
    else: # Curation-style is 'space'
        if (myargs.delete_older or myargs.close_older or myargs.bloom_older or myargs.optimize):
            success = False
            messages.append('Cannot specify --curation-style "space" and any of --delete, --close, --bloom or --optimize')
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

def get_indices(client, prefix='logstash-', exclude_pattern=None):
    """Return a sorted list of indices matching prefix"""
    _indices = sorted(client.indices.get_settings(index=prefix+'*', params={'expand_wildcards': 'closed'}).keys())
    if exclude_pattern:
        pattern = re.compile(exclude_pattern)
        return filter(lambda x: not pattern.search(x), _indices)
    else:
        return _indices
    
def get_version(client):
    """Return ES version number as a tuple"""
    version = client.info()['version']['number']
    return tuple(map(int, version.split('.')))

def find_expired_indices(client, time_unit, unit_count, separator='.', prefix='logstash-', utc_now=None, exclude_pattern=None):
    """ Generator that yields expired indices.

    :return: Yields tuples on the format ``(index_name, expired_by)`` where index_name
        is the name of the expired index and expired_by is the interval (timedelta) that the
        index was expired by.
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
    index_list = get_indices(client, prefix, exclude_pattern)

    for index_name in index_list:

        unprefixed_index_name = index_name[len(prefix):]

        # find the timestamp parts (i.e ['2011', '01', '05'] from '2011.01.05') using the configured separator
        parts = unprefixed_index_name.split(separator)

        # verify we have a valid cutoff - hours for 4-part indices, days for 3-part
        if len(parts) != required_parts:
            logger.debug('Skipping {0} because it is of a type (hourly or daily) that I\'m not asked to evaluate.'.format(index_name))
            continue

        try:
            index_time = get_index_time(unprefixed_index_name, separator=separator)
        except ValueError:
            logger.error('Could not find a valid timestamp from the index: {0}'.format(index_name))
            continue

        # if the index is older than the cutoff
        if index_time < cutoff:
            yield index_name, cutoff-index_time

        else:
            logger.info('{0} is {1} above the cutoff.'.format(index_name, index_time-cutoff))


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
    'close': (_close_index, {'op': 'close', 'verbed': 'closed', 'gerund': 'Closing'}),
    'delete': (_delete_index, {'op': 'delete', 'verbed': 'deleted', 'gerund': 'Deleting'}),
    'optimize': (_optimize_index, {'op': 'optimize', 'verbed': 'optimized', 'gerund': 'Optimizing'}),
    'bloom': (_bloom_index, {'op': 'disable bloom filter for', 'verbed': 'bloom filter disabled', 'gerund': 'Disabling bloom filter for'}),
    'require': (_require_index, {'op': 'update require allocation rules for', 'verbed':'index routing allocation updated', 'gerund': 'Updating required index routing allocation rules for'}),
}

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
            logger.info('Attempting {0} index {1} due to space constraints.'.format(words['gerund'].lower(), index_name))

        skipped = op(client, index_name, **kwargs)

        if skipped:
            continue

        # if no error was raised and we got here that means the operation succeeded
        logger.info('{0}: Successfully {1}.'.format(index_name, words['verbed']))
    logger.info('{0} index operations completed.'.format(words['op'].upper()))


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


def main():
    start = time.time()

    parser = make_parser()
    arguments = parser.parse_args()

    # Do not log and force dry-run if we opt to show indices.
    if arguments.show_indices:
        arguments.log_file = '/dev/null'
        arguments.dry_run = True

    # Setup logging
    if arguments.debug:
        numeric_log_level = logging.DEBUG
    else:
        numeric_log_level = getattr(logging, arguments.log_level.upper(), None)
        if not isinstance(numeric_log_level, int):
            raise ValueError('Invalid log level: %s' % loglevel)

    logging.basicConfig(level=numeric_log_level,
                        format='%(asctime)s.%(msecs)03d %(levelname)-9s %(funcName)22s:%(lineno)-4d %(message)s',
                        datefmt="%Y-%m-%dT%H:%M:%S",
                        stream=open(arguments.log_file, 'a') if arguments.log_file else sys.stderr)
    logging.info("Job starting...")

    # Setting up NullHandler to handle nested elasticsearch.trace Logger instance in elasticsearch python client
    logging.getLogger('elasticsearch.trace').addHandler(NullHandler())

    if arguments.show_indices:
        pass # Skip checking args if we're only showing indices
    else:
        check_args = validate_args(arguments) # Returns either True or a list of errors
        if not check_args == True:
            logger.error('Malformed arguments: {0}'.format(';'.join(check_args)))
            parser.print_help()
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
        for index_name in get_indices(client, arguments.prefix, arguments.exclude_pattern):
            print('{0}'.format(index_name))
        sys.exit(0)
    # Delete by space first
    if arguments.disk_space:
        logger.info('Deleting indices by disk usage over {0} gigabytes'.format(arguments.disk_space))
        expired_indices = find_overusage_indices(client, arguments.disk_space, arguments.separator, arguments.prefix)
        index_loop(client, 'delete', expired_indices, arguments.dry_run, by_space=True)
    # Delete by time
    if arguments.delete_older:
        logger.info('Deleting indices older than {0} {1}...'.format(arguments.delete_older, arguments.time_unit))
        expired_indices = find_expired_indices(client, time_unit=arguments.time_unit, unit_count=arguments.delete_older, separator=arguments.separator, 
                                               prefix=arguments.prefix, exclude_pattern=arguments.exclude_pattern)
        index_loop(client, 'delete', expired_indices, arguments.dry_run)
    # Close by time
    if arguments.close_older:
        logger.info('Closing indices older than {0} {1}...'.format(arguments.close_older, arguments.time_unit))
        expired_indices = find_expired_indices(client, time_unit=arguments.time_unit, unit_count=arguments.close_older, separator=arguments.separator,
                                               prefix=arguments.prefix, exclude_pattern=arguments.exclude_pattern)
        index_loop(client, 'close', expired_indices, arguments.dry_run)
    # Disable bloom filter by time
    if arguments.bloom_older:
        logger.info('Disabling bloom filter on indices older than {0} {1}...'.format(arguments.bloom_older, arguments.time_unit))
        expired_indices = find_expired_indices(client, time_unit=arguments.time_unit, unit_count=arguments.bloom_older, separator=arguments.separator,
                                               prefix=arguments.prefix, exclude_pattern=arguments.exclude_pattern)
        index_loop(client, 'bloom', expired_indices, arguments.dry_run)
    # Optimize index
    if arguments.optimize:
        logger.info('Optimizing indices older than {0} {1}...'.format(arguments.optimize, arguments.time_unit))
        expired_indices = find_expired_indices(client, time_unit=arguments.time_unit, unit_count=arguments.optimize, separator=arguments.separator,
                                               prefix=arguments.prefix, exclude_pattern=arguments.exclude_pattern)
        index_loop(client, 'optimize', expired_indices, arguments.dry_run, max_num_segments=arguments.max_num_segments)
    # Required routing rules
    if arguments.require:
        logger.info('Updating required routing allocation rules on indices older than {0} {1}...'.format(arguments.require, arguments.time_unit))
        expired_indices = find_expired_indices(client, time_unit=arguments.time_unit, unit_count=arguments.require, separator=arguments.separator,
                                               prefix=arguments.prefix, exclude_pattern=arguments.exclude_pattern)
        index_loop(client, 'require', expired_indices, arguments.dry_run, attr=arguments.required_rule)


    logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))


if __name__ == '__main__':
    main()
