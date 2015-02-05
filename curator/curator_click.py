#!/usr/bin/env python

import click
import sys

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


def get_regex(rawprefix, rawsuffix, rawtimestring, all_flag=False):
    """Get the regex which will be used to match indices"""

    match_all = False

    prefix     = rawprefix if rawprefix else ''
    suffix     = rawsuffix if rawsuffix else ''
    timestring = rawtimestring if rawtimestring else ''

    if not prefix and not suffix and not timestring:
        print('ERROR: No prefix, suffix or timestring specified.  Exiting.')
        sys.exit(1)
    # Catch cases where all indices may be globbed.
    if prefix == '.*' and not timestring and not suffix:
        match_all = True
    if suffix == '.*' and not timestring and not prefix:
        match_all = True
    if match_all and not all_flag:
        print('ERROR: The given pattern, "{0}" will match all indices but the --all-indices flag was not present.')
        print('The --all-indices flag is a safety feature to prevent accidentally acting on all indices.')
        sys.exit(1)
    regex = '^' + prefix + timestring + suffix + '$'
    return regex

@click.command()
@click.option('--newer-than', type=int,
                help='Include only indices newer than n days')
@click.option('--older-than', type=int,
                help='Include only indices older than n days')
@click.option('--prefix', help='Include only indices with this prefix')
@click.option('--suffix', help='Include only indices with this suffix')
@click.option('--time-unit',
                type=click.Choice(['hours', 'days', 'weeks', 'months']),
                help='Unit of time to reckon by')
@click.option('--timestring',
                help="Python strftime string to match your index definition, e.g. 2014.07.15 would be %%Y.%%m.%%d")
@click.option('--exclude', help='Exclude indices matching the provided value.')
@click.option('--all-indices', is_flag=True,
                help='Do not filter indices.  Act on all indices.')
@click.pass_context
def indices(ctx, newer_than, older_than, prefix, suffix, time_unit,
            timestring, exclude, all_indices):
    """Provide a filtered list of indices."""
    # This is also the action part of the script now, where an action requires
    # a list of indices to act on.
    startlen = len(ilist)
    if all_indices:
        click.echo('Match all indices. Ignoring all other flags.')
        prefix = '.*'
        suffix = ''
        timestring = ''
    else:
        if newer_than:
            click.echo('Filter newer than {0}'.format(newer_than))
            ilist.pop(-1)
        if older_than:
            click.echo('Filter older than {0}'.format(older_than))
            ilist.pop(0)
        if prefix:
            click.echo('Include only prefix {0}'.format(prefix))
            ilist.pop(3)
        if suffix:
            click.echo('Include only suffix {0}'.format(suffix))
            ilist.pop(-3)
        if time_unit:
            click.echo('Time unit for filtering: {0}'.format(time_unit))
        if timestring:
            click.echo('Time string to match: {0}'.format(timestring))
        if exclude:
            click.echo('Exclude indices matching {0}'.format(exclude))
            ilist.pop(-2)
        if startlen == len(ilist):
            # No changes to the list, and all_indices isn't true.
            # This means no args were passed :(
            click.echo("ERROR: No filters applied, but nofilter was not selected.")
            click.echo("Exiting...")
            sys.exit(1)
    print('Index pattern regex is {0}'.format(get_regex(prefix, suffix, timestring, all_flag=all_indices)))
    print('We will do action: {0} with index list: {1}'.format(ctx.parent.info_name, ilist))
    if ctx.parent.info_name == 'snapshot':
        if 'no_wait_for_completion' in ctx.parent.params.keys():
            wait_for_completion = False
        else:
            wait_for_completion = True
            print("Wait for completion? : {0}".format(wait_for_completion))
    print("CONTEXT: {0}".format(ctx.parent.params))
    print("CONTEXT2: {0}".format(ctx.parent.parent.params))

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

@cli.group('alias')
@click.option('--remove', is_flag=True, show_default=True, expose_value=True,
            help='Remove from alias rather than add.')
@click.pass_context
def alias(ctx, remove):
    """Index Aliasing"""
    if remove:
        click.echo("Remove from alias")
    else:
        click.echo("Add to alias")
alias.add_command(indices)

@cli.group('allocation')
@click.option('--rule', show_default=True, expose_value=True, type=str, required=True,
            help='Routing allocation rule to apply, e.g. tag=ssd')
@click.pass_context
def allocation(ctx, rule):
    """Index Allocation"""
    click.echo("Index Allocation")
allocation.add_command(indices)

@cli.group('bloom')
@click.option('--delay', type=int, default=0, show_default=True, expose_value=True,
            help='Number of seconds to delay after disabling bloom filter cache of an index')
@click.pass_context
def bloom(ctx, delay):
    """Disable bloom filter cache"""
    if delay > 0:
        click.echo("Add a delay between iterations")
    click.echo("Disable bloom filter cache")
bloom.add_command(indices)

@cli.group('close')
@click.pass_context
def close(ctx):
    """Close indices"""
    click.echo("Close indices")
close.add_command(indices)

@cli.group('delete')
@click.option('--disk-space', type=float, expose_value=True,
            help='Delete indices beyond DISK_SPACE gigabytes.')
@click.pass_context
def delete(ctx, disk_space):
    """Delete indices or snapshots"""
    if disk_space:
        #ctx.obj = {"disk_space" : True}
        click.echo("Delete by space")
    else:
        click.echo("Delete by filter")
delete.add_command(indices)
delete.add_command(snapshots)

@cli.group('optimize')
@click.option('--delay', type=int, default=0, show_default=True, expose_value=True,
            help='Number of seconds to delay after disabling bloom filter cache of an index')
@click.pass_context
def optimize(ctx, delay):
    """Optimize Indices"""
    if delay > 0:
        click.echo("Add a delay between iterations")
    click.echo("Optimize Indices")
optimize.add_command(indices)

@cli.group('replicas')
@click.option('--count', type=int, default=1, show_default=True, expose_value=True,
            help='Number of replicas the indices should have.', required=True)
@click.pass_context
def replicas(ctx, count):
    """Replica Count Per-shard"""
    click.echo("Replica Count Per-shard")
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
    click.echo("Take snapshots of indices (Backup)")
    if no_wait_for_completion:
        wait_for_completion = False
    else:
        wait_for_completion = True
snapshot.add_command(indices)

cli()
print('END: ilist = {0}'.format(ilist))
