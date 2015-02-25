from . import *
import elasticsearch
import click
import re

import logging
logger = logging.getLogger(__name__)

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
