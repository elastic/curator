from . import *
from ..api import *
import elasticsearch
import click
import re

import logging
logger = logging.getLogger(__name__)


### SNAPSHOTS
@click.command(short_help="Snapshot selection.")
@click.option('--newer-than', type=int, callback=filter_callback,
                help='Include only snapshots newer than n time_units')
@click.option('--older-than', type=int, callback=filter_callback,
                help='Include only snapshots older than n time_units')
@click.option('--prefix', type=str, callback=filter_callback,
                help='Include only snapshots beginning with prefix.')
@click.option('--suffix', type=str, callback=filter_callback,
                help='Include only snapshots ending with suffix.')
@click.option('--time-unit', is_eager=True,
                type=click.Choice(['hours', 'days', 'weeks', 'months']),
                help='Unit of time to reckon by')
@click.option('--timestring', type=str, is_eager=True, default='%Y%m%d%H%M%S',
                help="Python strftime string to match your snapshot's definition, e.g. 20140715020304 would be %Y%m%d%H%M%S")
@click.option('--regex', type=str, callback=filter_callback,
                help="Provide your own regex, e.g '^prefix-.*-suffix$'")
@click.option('--exclude', multiple=True, callback=filter_callback,
                help='Exclude matching snapshots. Can be invoked multiple times.')
@click.option('--snapshot', multiple=True,
                help='Include the provided snapshot in the list. Can be invoked multiple times.')
@click.option('--all-snapshots', is_flag=True,
                help='Do not filter snapshots.  Act on all snapshots.')
@click.option('--repository', type=str, is_eager=True, expose_value=True,
                help='Repository name.')
@click.pass_context
def snapshots(ctx, newer_than, older_than, prefix, suffix, time_unit,
            timestring, regex, exclude, snapshot, all_snapshots, repository):
    """
    Get a list of snapshots to act on from the provided arguments, then perform
    the command [delete, show] on the resulting list.

    """

    if not repository:
        click.echo('{0}'.format(ctx.get_help()))
        click.echo(click.style('Missing required --repository parameter.', fg='red', bold=True))
        sys.exit(1)

    logger.info("Job starting: {0} snapshots".format(ctx.parent.info_name))

    # Base and client args are in the grandparent tier of the context
    if ctx.parent.parent.params['dry_run']:
        logging.info("DRY RUN MODE.  No changes will be made.")

    client = get_client(**ctx.parent.parent.params)
    # Get a master-list of indices
    snapshots = get_snapshots(client, repository=repository)
    if snapshots:
        working_list = snapshots
    else:
        click.echo(click.style('ERROR. No snapshots found in Elasticsearch.', fg='red', bold=True))
        sys.exit(1)

    if all_snapshots:
        logger.info('Matching all snapshots. Ignoring flags other than --exclude.')
    else:
        logger.debug('All filters: {0}'.format(ctx.obj['filters']))

    for f in ctx.obj['filters']:
        if all_snapshots and not 'exclude' in f:
            continue
        logger.debug('Filter: {0}'.format(f))
        working_list = apply_filter(working_list, **f)

    # If there are manually added snapshots, we will add them here
    working_list.extend(in_list(snapshot, snapshots))

    if working_list:
        # Make a sorted, unique list of indices
        working_list = sorted(list(set(working_list)))
        logger.debug('ACTION: {0} will be executed against the following snapshots: {1}'.format(ctx.parent.info_name, working_list))
        if ctx.parent.info_name == 'show':
            show(working_list)
        elif ctx.parent.parent.params['dry_run']:
            show_dry_run(working_list, ctx.parent.info_name)
        elif ctx.parent.info_name == 'delete':
            success = True
            for snap in working_list:
                retval = delete_snapshot(client, snapshot=snap, repository=repository)
                # If we fail once, we fail completely
                if not retval:
                    success = False
            exit_msg(success)

    else:
        logger.warn('No snapshots matched provided args.')
        click.echo(click.style('ERROR. No snapshots matched provided args.', fg='red', bold=True))
        sys.exit(99)
