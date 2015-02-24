#!/usr/bin/env python

import click
import sys
import os
import time
import logging
from datetime import timedelta, datetime, date
import json
from collections import OrderedDict

import elasticsearch
import curator

try:
    from logging import NullHandler
except ImportError:
    from logging import Handler

    class NullHandler(Handler):
        def emit(self, record):
            pass

__version__ = '3.0.0-dev'

# Elasticsearch versions supported
version_max  = (2, 0, 0)
version_min = (1, 0, 0)

logger = logging.getLogger(__name__)

has_indices = False
ilist = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o']
slist = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o']


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
    'no_wait_for_completion': False,
    'ignore_unavailable': False,
    'include_global_state': True,
    'partial': False,
}

class LogstashFormatter(logging.Formatter):
    # The LogRecord attributes we want to carry over to the Logstash message,
    # mapped to the corresponding output key.
    WANTED_ATTRS = {'levelname': 'loglevel',
                    'funcName': 'function',
                    'lineno': 'linenum',
                    'message': 'message',
                    'name': 'name'}

    def converter(self, timevalue):
        return time.gmtime(timevalue)

    def format(self, record):
        timestamp = '%s.%03dZ' % (
            self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S'), record.msecs)
        result = {'message': record.getMessage(),
                  '@timestamp': timestamp}
        for attribute in set(self.WANTED_ATTRS).intersection(record.__dict__):
            result[self.WANTED_ATTRS[attribute]] = getattr(record, attribute)
        return json.dumps(result, sort_keys=True)

class Whitelist(logging.Filter):
    def __init__(self, *whitelist):
        self.whitelist = [logging.Filter(name) for name in whitelist]

    def filter(self, record):
        return any(f.filter(record) for f in self.whitelist)

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

def show_vars(ctx, param, value):
    """Showing the local variables passed"""
    logger.debug("HEY! {0} has a value of {1}".format(param.name, value))
    logger.debug("CTX! {0}".format(ctx.params))
    return value

def get_client(ctx):
    """Return an Elasticsearch client using context parameters

    """
    d = ctx.parent.parent.params
    logger.debug("Client CTX: {0}".format(d))

    try:
        client = elasticsearch.Elasticsearch(
            host=d["host"], http_auth=d["auth"], port=d["port"],
            url_prefix=d["url_prefix"], timeout=d["timeout"], use_ssl=d["ssl"]
            )
        # Verify the version is acceptable.
        check_version(client)
        # Verify "master_only" status, if applicable
        if d["master_only"] and not curator.is_master_node(client):
            logger.info('Master-only flag detected. Connected to non-master node. Aborting.')
            sys.exit(9)
        return client
    except:
        print("ERROR: Connection failure.  Exiting.")
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

def validate_args(ctx):
    """
    Validate possibly conflicting arguments
    """
    error = False
    p = ctx.params
    logger.debug("p = {0}".format(p))
    if p['time_unit']:
        if not p['timestring']:
            logger.error("Must provide timestring with --time-unit")
            error = True
        else:
            logger.debug('Time unit for filtering: {0}'.format(p['time_unit']))
    if p['newer_than']:
        if not p['timestring']:
            logger.error("Must provide timestring with --newer-than")
            error = True
        if not p['time_unit']:
            logger.error("Must provide time_unit with --newer-than")
            error = True
    if p['older_than']:
        if not p['timestring']:
            logger.error("Must provide timestring with --older-than")
            error = True
        if not p['time_unit']:
            logger.error("Must provide time_unit with --older-than")
            error = True
    return error

def validate_timeout(command, timeout, timeout_override=False):
    """Validate client connection args. Correct where necessary."""
    # Override the timestamp in case the end-user doesn't.
    replacement_timeout = 21600
    # val is arbitrarily set at two hours right now.
    if command == "optimize": # This is for Elasticsearch < 1.5
        val = 7200
    elif command == "snapshot":
        val = 7200
    if timeout_override and timeout < val:
        logger.info('Timeout of {0} seconds is too low for command {1}.  Overriding to {2} seconds.'.format(timeout, command.upper(), replacement_timeout))
        timeout = replacement_timeout
    return timeout



#######################################
### This is where the magic happens ###
#######################################
@click.command()
@click.option('--newer-than', type=int,
                help='Include only indices newer than n time_units')
@click.option('--older-than', type=int,
                help='Include only indices older than n time_units')
@click.option('--prefix', type=str,
                help='Include only indices beginning with prefix.')
@click.option('--suffix', type=str,
                help='Include only indices ending with suffix.')
@click.option('--time-unit',
                type=click.Choice(['hours', 'days', 'weeks', 'months']),
                help='Unit of time to reckon by')
@click.option('--timestring', type=str,
                help="Python strftime string to match your index definition, e.g. 2014.07.15 would be %%Y.%%m.%%d")
@click.option('--regex', type=str,
                help="Provide your own regex.  Must be in python re style, e.g. r'^prefix-.*-suffix$'")
@click.option('--exclude', multiple=True,
                help='Exclude matching indices. Can be invoked multiple times.')
@click.option('--index', multiple=True,
                help='Include the provided index in the list. Can be invoked multiple times.')
@click.option('--all-indices', is_flag=True,
                help='Do not filter indices.  Act on all indices.')
@click.pass_context
def indices(ctx, newer_than, older_than, prefix, suffix, time_unit,
            timestring, regex, exclude, index, all_indices):
    """Provide a filtered list of indices."""
    # This is also the action part of the script now, where an action requires
    # a list of indices to act on.

    # The primary args are the root-level ones, for Elasticsearch and the main
    # program
    primary_args = ctx.parent.parent.params
    logging.debug("primary_args: {0}".format(primary_args))
    # The action args are for the selected command
    action_args = ctx.parent.params
    logging.debug("action_args: {0}".format(action_args))
    local_args = locals()
    logging.debug("locals: {0}".format(local_args))

    # Do simple exit conditions before connecting client
    has_errors = validate_args(ctx)
    if has_errors:
        sys.exit(1)

    logging.info("Job starting...")

    if primary_args["dry_run"]:
        logging.info("DRY RUN MODE.  No changes will be made.")

    client = get_client(ctx)

    # Get a master-list of indices
    _indices = sorted(client.indices.get_settings(
        index='*', params={'expand_wildcards': 'open,closed'}).keys()
        )
    logger.debug("Full list of indices: {0}".format(_indices))

    filtered = _indices

    if all_indices:
        logger.info('Matching all indices. Ignoring flags other than --exclude.')
    else:
        if prefix:
            logger.info('Include only prefix {0}'.format(prefix))
            myregex = r'^{0}.*$'.format(prefix)
            filtered = curator.regex_iterate(filtered, myregex)
            logger.debug("Filtered index list: {0}".format(filtered))
        if suffix:
            logger.info('Include only suffix {0}'.format(suffix))
            myregex = r'^.*{0}$'.format(suffix)
            filtered = curator.regex_iterate(filtered, myregex)
            logger.debug("Filtered index list: {0}".format(filtered))

        # It is possible to want to filter based on the presence
        # of a timestring, but not based on date, so we don't exit here.
        if timestring:
            click.echo('Time string to match: {0}'.format(timestring))

        if newer_than or older_than:
            if not filtered:
                filtered = _indices

        if newer_than:
            date_regex = curator.get_date_regex(timestring)
            myregex = r'^.*(?P<date>{0}).*$'.format(date_regex)
            filtered = curator.regex_iterate(
                filtered, myregex, groupname="date", timestring=timestring,
                time_unit=time_unit, method="newer_than", value=newer_than
                )
            logger.info('Filter newer than {0}'.format(newer_than))

        if older_than:
            date_regex = curator.get_date_regex(timestring)
            myregex = r'^.*(?P<date>{0}).*$'.format(date_regex)
            filtered = curator.regex_iterate(
                filtered, myregex, groupname="date", timestring=timestring,
                time_unit=time_unit, method="older_than", value=older_than
                )
            logger.info('Filter older than {0}'.format(older_than))

    for e in exclude:
        # Exclude on filtered
        logger.info('Excluding indices matching {0}'.format(e))
        pattern = re.compile(e)
        filtered = list(filter(lambda x: not pattern.search(x), filtered))

    filtered = sorted(list(OrderedDict.fromkeys(filtered)))
    filtered = curator.prune_kibana(filtered)
    logger.debug("Pruned list of indices: {0}".format(filtered))

    if index:
        logger.debug("Manually adding indices specified by --index argument(s)")
        filtered.extend(index)

    logger.debug('ACTION: {0} will be executed against the following indices: {1}'.format(ctx.parent.info_name, filtered))
    # This goofy turnaround keeps the args looking sane, but makes sense
    # programmatically
    if ctx.parent.info_name == 'snapshot':
        if 'no_wait_for_completion' in ctx.parent.params.keys():
            wait_for_completion = False
        else:
            wait_for_completion = True
            print("Wait for completion? : {0}".format(wait_for_completion))

# Snapshots
@click.command()
@click.option('--repository', help='Repository name', required=True)
@click.option('--newer-than', type=int,
                help='Include only snapshots newer than n days')
@click.option('--older-than', type=int,
                help='Include only snapshots older than n days')
@click.option('--prefix', help='Include only snapshots with this prefix')
@click.option('--suffix', help='Include only snapshots with this suffix')
@click.option('--time-unit',
                type=click.Choice(['hours', 'days', 'weeks', 'months']),
                help='Unit of time to reckon by')
@click.option('--timestring',
                help="Python strftime string to match your index definition, e.g. 2014.07.15 would be %%Y.%%m.%%d")
@click.option('--exclude', help='Exclude snapshots matching the provided value.')
@click.option('--nofilter', is_flag=True,
                help='Do not filter snapshots.  Act on all snapshots.')
@click.pass_context
def snapshots(ctx, newer_than, older_than, prefix, suffix, exclude, nofilter):
    """Provide a filtered list of snapshots."""
    #print('CONTEXT: {0}'.format(ctx.obj))
    if ctx.obj["disk_space"]:
        click.echo('ERROR: Cannot use "--disk-space" parameter for snapshot operations.')
        click.echo("Exiting...")
        sys.exit(1)
    startlen = len(slist)
    if nofilter:
        click.echo('Will not filter.  Using all snapshots.')
        return
    if newer_than:
        click.echo('Filter newer than {0}'.format(newer_than))
        slist.pop(-1)
    if older_than:
        click.echo('Filter older than {0}'.format(older_than))
        slist.pop(0)
    if prefix:
        click.echo('Include only prefix {0}'.format(prefix))
        slist.pop(3)
    if suffix:
        click.echo('Include only suffix {0}'.format(suffix))
        slist.pop(-3)
    if exclude:
        click.echo('Exclude snapshots matching {0}'.format(exclude))
        slist.pop(-2)
    if startlen == len(slist):
        # No changes to the list, and nofilter isn't true.
        # This means no args were passed :(
        click.echo("ERROR: No filters applied, but nofilter was not selected.")
        click.echo("Exiting...")
        sys.exit(1)
    print('We will do action: {0} with snapshot list: {1}'.format(ctx.parent.info_name, slist))


@click.group()
@click.option('--host', help='Elasticsearch host.', default=DEFAULT_ARGS['host'])
@click.option('--url_prefix', help='Elasticsearch http url prefix.', default=DEFAULT_ARGS['url_prefix'])
@click.option('--port', help='Elasticsearch port.', default=DEFAULT_ARGS['port'], type=int)
@click.option('--ssl', help='Connect to Elasticsearch through SSL.', is_flag=True, default=DEFAULT_ARGS['ssl'])
@click.option('--auth', help='Use Basic Authentication ex: user:pass', default=DEFAULT_ARGS['auth'])
@click.option('--timeout', help='Connection timeout in seconds.', default=DEFAULT_ARGS['timeout'], type=int)
@click.option('--master-only', is_flag=True, help='Only operate on elected master node.')
@click.option('--dry-run', is_flag=True, help='Do not perform any changes.', default=DEFAULT_ARGS['dry_run'])
@click.option('--debug', is_flag=True, help='Debug mode', default=DEFAULT_ARGS['debug'])
@click.option('--loglevel', help='Log level', default=DEFAULT_ARGS['log_level'])
@click.option('--logfile', help='log file')
@click.option('--logformat', help='Log output format [default|logstash].', default=DEFAULT_ARGS['logformat'])
@click.pass_context
def cli(ctx, host, url_prefix, port, ssl, auth, timeout, master_only, dry_run, debug, loglevel, logfile, logformat):
    """Curator for Elasticsearch indices. See http://github.com/elasticsearch/curator/wiki
    """
    # Setup logging
    if debug:
        numeric_log_level = logging.DEBUG
        format_string = '%(asctime)s %(levelname)-9s %(name)22s %(funcName)22s:%(lineno)-4d %(message)s'
    else:
        numeric_log_level = getattr(logging, loglevel.upper(), None)
        format_string = '%(asctime)s %(levelname)-9s %(message)s'
        if not isinstance(numeric_log_level, int):
            raise ValueError('Invalid log level: {0}'.format(loglevel))

    handler = logging.StreamHandler(
        open(logfile, 'a') if logfile else sys.stderr)
    if logformat == 'logstash':
        handler.setFormatter(LogstashFormatter())
    else:
        handler.setFormatter(logging.Formatter(format_string))
    logging.root.addHandler(handler)
    logging.root.setLevel(numeric_log_level)

    # Filter out logging from Elasticsearch and associated modules by default
    if not debug:
        for handler in logging.root.handlers:
            handler.addFilter(Whitelist('root', '__main__', 'curator', 'curator.curator'))

    # Setting up NullHandler to handle nested elasticsearch.trace Logger instance in elasticsearch python client
    logging.getLogger('elasticsearch.trace').addHandler(NullHandler())



@cli.group('alias')
@click.option('--name', required=True, help="Alias name", type=str)
@click.option('--remove', is_flag=True, show_default=True, expose_value=True,
            help='Remove from alias rather than add.')
@click.pass_context
def alias(ctx, name, remove):
    """Index Aliasing"""
    logging.debug("ACTION: Alias indices")
    if remove:
        logging.debug("CONFIGURATION: Remove indices from {0}".format(name))
    else:
        logging.debug("CONFIGURATION: Add indices to {0}".format(name))
alias.add_command(indices)

@cli.group('allocation')
@click.option('--rule', show_default=True, expose_value=True, type=str, required=True,
            help='Routing allocation rule to apply, e.g. tag=ssd')
@click.pass_context
def allocation(ctx, rule):
    """Index Allocation"""
    logging.debug("ACTION: Index Allocation")
    logging.debug("CONFIGURATION: rule = {0}".format(rule))
allocation.add_command(indices)

@cli.group('bloom')
@click.option('--delay', type=int, default=0, show_default=True, expose_value=True,
            help='Number of seconds to delay after disabling bloom filter cache of an index')
@click.pass_context
def bloom(ctx, delay):
    """Disable bloom filter cache"""
    ctx.obj = {"timeout_override" : True}
    logging.debug("ACTION: Disable bloom filter cache")
    if delay > 0:
        logging.debug("CONFIGURATION: Add a {0} second delay between iterations".format(delay))
bloom.add_command(indices)

@cli.group('close')
@click.pass_context
def close(ctx):
    """Close indices"""
    logging.debug("ACTION: Close indices")
close.add_command(indices)

@cli.group('delete')
@click.option('--disk-space', type=float, expose_value=True,
            help='Delete indices beyond DISK_SPACE gigabytes.')
@click.pass_context
def delete(ctx, disk_space):
    """Delete indices or snapshots"""
    logging.debug("ACTION: Delete indices")
    if disk_space:
        #ctx.obj = {"disk_space" : True}
        logging.debug("CONFIGURATION: Delete by space")
    else:
        logging.debug("CONFIGURATION: Delete by filter")
delete.add_command(indices)
delete.add_command(snapshots)

@cli.group('optimize')
@click.option('--delay', type=int, default=0, show_default=True, expose_value=True,
            help='Number of seconds to delay after disabling bloom filter cache of an index')
@click.pass_context
def optimize(ctx, delay):
    """Optimize Indices"""
    ctx.obj = {"timeout_override" : True}
    logging.debug("ACTION: Optimize Indices")
    if delay > 0:
        logging.debug("CONFIGURATION: Add a {0} second delay between iterations".format(delay))
optimize.add_command(indices)

@cli.group('replicas')
@click.option('--count', type=int, default=1, show_default=True, expose_value=True,
            help='Number of replicas the indices should have.', required=True)
@click.pass_context
def replicas(ctx, count):
    """Replica Count Per-shard"""
    logging.debug("ACTION: Update Replica Count Per-shard to {0}".format(count))
replicas.add_command(indices)

@cli.group('snapshot')
@click.option('--repository', help='Repository name.', expose_value=True, required=True)
@click.option('--snapshot-name', help='Override default name.', expose_value=True)
@click.option('--snapshot-prefix', help='Override default prefix.',
            expose_value=True, default=DEFAULT_ARGS['snapshot_prefix'])
@click.option('--no_wait_for_completion', is_flag=True, expose_value=True,
            help='Do not wait for snapshot to complete before returning.')
@click.option('--ignore_unavailable', is_flag=True, show_default=True, expose_value=True,
            help='Ignore unavailable shards/indices.', default=DEFAULT_ARGS['ignore_unavailable'])
@click.option('--include_global_state', is_flag=True, show_default=True,
            expose_value=True, help='Store cluster global state with snapshot.')
@click.option('--partial', is_flag=True, show_default=True, expose_value=True,
            help='Do not fail if primary shard is unavailable.')
@click.pass_context
def snapshot(ctx, repository, snapshot_name, snapshot_prefix, no_wait_for_completion, ignore_unavailable, include_global_state, partial):
    """Take snapshots of indices (Backup)"""
    ctx.obj = {"timeout_override" : True}
    logging.debug("ACTION: Take snapshots of indices (Backup)")
    if no_wait_for_completion:
        wait_for_completion = False
    else:
        wait_for_completion = True
snapshot.add_command(indices)

def main():
    start = time.time()

    # Run the CLI!
    cli()

    logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))

if __name__ == '__main__':
    main()
