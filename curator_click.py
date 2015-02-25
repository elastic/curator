#!/usr/bin/env python

import click
import sys
import os
import re
import time
import logging
from datetime import timedelta, datetime, date
import json
#from collections import OrderedDict

import elasticsearch
import curator

try:
    from logging import NullHandler
except ImportError:
    from logging import Handler

    class NullHandler(Handler):
        def emit(self, record):
            pass

from curator import __version__

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
    'no_wait_for_completion': False,
    'ignore_unavailable': False,
    'include_global_state': True,
    'partial': False,
}

REGEX_MAP = {
    'timestring': r'^.*{0}.*$',
    'newer_than': r'^.*(?P<date>{0}).*$',
    'older_than': r'^.*(?P<date>{0}).*$',
    'prefix': r'^{0}.*$',
    'suffix': r'^.*{0}$',
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
    logger.debug("CTX! {0} has a value of {1} and the CTX contains: {2}".format(param.name, value, ctx.params))
    return value

def get_client(ctx):
    """Return an Elasticsearch client using context parameters

    """
    d = ctx.params
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

def filter_any(param):
    """Test all flags to see if any filtering is being done"""
    retval = False
    if param.name == "regex":
        return True
    if param.name == "prefix":
        return True
    if param.name == "suffix":
        return True
    if param.name == "newer_than":
        return True
    if param.name == "older_than":
        return True
    if param.name == "timestring":
        return True
    if param.name == "exclude":
        return True
    return retval

def filter_callback(ctx, param, value):
    """
    Filter ctx.obj["filtered"] based on what shows up here.
    """
    # Stop here if None or empty value
    if not value:
        return value
    else:
        kwargs = {}

    # If we're calling a filtered object and ctx.obj['filtered'] is empty we
    # need to copy over ctx.obj['indices']
    if not ctx.obj["filtered"]:
        if filter_any(param):
            ctx.obj["filtered"] = ctx.obj["indices"]

    if param.name in ['older_than', 'newer_than']:
        kwargs = {  "groupname":'date', "time_unit":ctx.params["time_unit"],
                    "timestring": ctx.params['timestring'], "value": value,
                    "method": param.name }
        date_regex = curator.get_date_regex(ctx.params['timestring'])
        regex = REGEX_MAP[param.name].format(date_regex)
    elif param.name == 'regex':
        regex = "r'{0}'".format(value)
    elif param.name in ['prefix', 'suffix']:
        regex = REGEX_MAP[param.name].format(value)

    if param.name == 'exclude':
        for e in value:
            logger.info('Excluding indices matching {0}'.format(e))
            pattern = re.compile(e)
            ctx.obj["filtered"] = list(filter(lambda x: not pattern.search(x), ctx.obj["filtered"]))
    else:
        logger.debug("REGEX = {0}".format(regex))
        ctx.obj["filtered"] = curator.regex_iterate(ctx.obj["filtered"], regex, **kwargs)
    logger.debug("Filtered index list: {0}".format(ctx.obj["filtered"]))
    return value

def filter_timestring_only(ctx, timestring):
    """
    Because Click will not allow option dependencies, this is a work-around
    just in case someone is using timestamp filtering without using
    ``older_than`` or ``newer_than``
    """
    # If we're calling a filtered object and ctx.obj['filtered'] is empty we
    # need to copy over ctx.obj['indices']
    if not ctx.obj["filtered"]:
        ctx.obj["filtered"] = ctx.obj["indices"]
    date_regex = curator.get_date_regex(timestring)
    regex = r'^.*{0}.*$'.format(date_regex)
    ctx.obj["filtered"] = curator.regex_iterate(ctx.obj["filtered"], regex)
    logger.debug("Filtered index list: {0}".format(ctx.obj["filtered"]))

def add_indices_callback(ctx, param, value):
    """
    Add indices (if they exist) to ctx.obj["add_indices"]
    They will be added to the actionable list just before the action is executed.
    """
    # Only add an index if it actually exists, hence we check the original
    # list here
    for i in value:
        if i in ctx.obj["indices"]:
            logger.info('Adding index {0} from command-line argument'.format(i))
            ctx.obj["add_indices"].append(i)
        else:
            logger.warn('Index {0} not found!'.format(i))
    return value

#######################################
### This is where the magic happens ###
#######################################
@click.command(short_help="Index selection.")
@click.option('--newer-than', type=int, callback=filter_callback,
                help='Include only indices newer than n time_units')
@click.option('--older-than', type=int, callback=filter_callback,
                help='Include only indices older than n time_units')
@click.option('--prefix', type=str, callback=filter_callback,
                help='Include only indices beginning with prefix.')
@click.option('--suffix', type=str, callback=filter_callback,
                help='Include only indices ending with suffix.')
@click.option('--time-unit', is_eager=True,
                type=click.Choice(['hours', 'days', 'weeks', 'months']),
                help='Unit of time to reckon by')
@click.option('--timestring', type=str, is_eager=True,
                help="Python strftime string to match your index definition, e.g. 2014.07.15 would be %%Y.%%m.%%d")
@click.option('--regex', type=str, callback=filter_callback,
                help="Provide your own regex, e.g '^prefix-.*-suffix$'")
@click.option('--exclude', multiple=True, callback=filter_callback,
                help='Exclude matching indices. Can be invoked multiple times.')
@click.option('--index', multiple=True, callback=add_indices_callback,
                help='Include the provided index in the list. Can be invoked multiple times.')
@click.option('--all-indices', is_flag=True,
                help='Do not filter indices.  Act on all indices.')
@click.pass_context
def indices(ctx, newer_than, older_than, prefix, suffix, time_unit,
            timestring, regex, exclude, index, all_indices):
    """
    Get a list of indices to act on from the provided arguments, then perform
    the command [alias, allocation, bloom, close, delete, etc.] on the resulting
    list.

    """

    action_list = []

    # Work-around if filtering by timestring without older_than or newer_than
    if timestring and not older_than and not newer_than:
        filter_timestring_only(ctx, timestring)

    # This effectively overrides any prior options and makes it use all indices.
    if all_indices:
        logger.info('Matching all indices. Ignoring flags other than --exclude.')
        ctx.obj["filtered"] = ctx.obj["indices"]
        for e in exclude:
            logger.info('Excluding indices matching {0}'.format(e))
            pattern = re.compile(e)
            ctx.obj["filtered"] = list(filter(lambda x: not pattern.search(x), ctx.obj["filtered"]))

    if ctx.obj["filtered"]:
        if ctx.parent.info_name == "delete": # Protect against accidental delete
            logger.info("Pruning Kibana-related indices to prevent accidental deletion.")
            ctx.obj["filtered"] = curator.prune_kibana(ctx.obj["filtered"])
        action_list.extend(ctx.obj["filtered"])

    if index:
        action_list.extend(ctx.obj["add_indices"])

    if action_list:
        # This ugly one liner makes a unique set, then into a sorted list of
        # indices to prevent actions from hitting the same index twice.
        action_list = sorted(list(set(action_list)))
        logger.debug('ACTION: {0} will be executed against the following indices: {1}'.format(ctx.parent.info_name, action_list))

        # This goofy turnaround keeps the args looking sane, but makes sense
        # programmatically.
        if ctx.parent.info_name == 'snapshot':
            if 'no_wait_for_completion' in ctx.params:
                wait_for_completion = False
            else:
                wait_for_completion = True

        # Do action here!!!
    else:
        logger.warn('No indices matched provided args.')
        click.echo(click.style('ERROR. No indices matched provided args.', fg='red', bold=True))
        sys.exit(99)


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
    # startlen = len(slist)
    # if nofilter:
    #     click.echo('Will not filter.  Using all snapshots.')
    #     return
    # if newer_than:
    #     click.echo('Filter newer than {0}'.format(newer_than))
    #     slist.pop(-1)
    # if older_than:
    #     click.echo('Filter older than {0}'.format(older_than))
    #     slist.pop(0)
    # if prefix:
    #     click.echo('Include only prefix {0}'.format(prefix))
    #     slist.pop(3)
    # if suffix:
    #     click.echo('Include only suffix {0}'.format(suffix))
    #     slist.pop(-3)
    # if exclude:
    #     click.echo('Exclude snapshots matching {0}'.format(exclude))
    #     slist.pop(-2)
    # if startlen == len(slist):
    #     # No changes to the list, and nofilter isn't true.
    #     # This means no args were passed :(
    #     click.echo("ERROR: No filters applied, but nofilter was not selected.")
    #     click.echo("Exiting...")
    #     sys.exit(1)
    # print('We will do action: {0} with snapshot list: {1}'.format(ctx.parent.info_name, slist))


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
@click.version_option(version=__version__)
@click.pass_context
def cli(ctx, host, url_prefix, port, ssl, auth, timeout, master_only, dry_run, debug, loglevel, logfile, logformat):
    """Curator for Elasticsearch indices. See http://github.com/elasticsearch/curator/wiki
    """
    # Check for --help, because we want a client created here otherwise
    args = " ".join(sys.argv)
    pattern = re.compile(r'^.*\-\-help.*$')
    wants_help = pattern.match(args)

    if not wants_help:
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

        logging.info("Job starting...")

        if dry_run:
            logging.info("DRY RUN MODE.  No changes will be made.")

        ctx.obj["client"] = get_client(ctx)

        # Get a master-list of indices
        ctx.obj["indices"] = sorted(ctx.obj["client"].indices.get_settings(
            index='*', params={'expand_wildcards': 'open,closed'}).keys()
            )
        logger.debug("All indices: {0}".format(ctx.obj["indices"]))

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
    ctx.obj["timeout_override"] = True
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
    ctx.obj["timeout_override"] = True
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
    ctx.obj["timeout_override"] = True
    logging.debug("ACTION: Take snapshots of indices (Backup)")
    if no_wait_for_completion:
        wait_for_completion = False
    else:
        wait_for_completion = True
snapshot.add_command(indices)

def main():
    start = time.time()

    # Run the CLI!
    cli(obj={"filtered": [], "add_indices": []})

    logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))

if __name__ == '__main__':
    main()
