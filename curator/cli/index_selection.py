from ..cli import *
from ..api import *
import elasticsearch
import click
import re

import logging
logger = logging.getLogger(__name__)

### INDICES
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
                help="Python strftime string to match your index definition, e.g. 2014.07.15 would be %Y.%m.%d")
@click.option('--regex', type=str, callback=filter_callback,
                help="Provide your own regex, e.g '^prefix-.*-suffix$'")
@click.option('--exclude', multiple=True, callback=filter_callback,
                help='Exclude matching indices. Can be invoked multiple times.')
@click.option('--index', multiple=True,
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
    # This top 'if' statement catches an edge-case I cannot depend upon click
    # to resolve.  I cannot make options depend upon each other (yet), so I
    # have to test for this case here and act accordingly.
    if timestring and not ctx.obj['filters']:
        regex = r'^.*{0}.*$'.format(get_date_regex(timestring))
        ctx.obj['filters'].append({ 'pattern': regex })
    if not all_indices and not ctx.obj['filters'] and not index:
        click.echo('{0}'.format(ctx.get_help()))
        click.echo(click.style('ERROR. At least one filter must be supplied.', fg='red', bold=True))
        sys.exit(1)

    logger.info("Job starting: {0} indices".format(ctx.parent.info_name))
    logger.debug("Params: {0}".format(ctx.parent.parent.params))
    # Base and client args are in the grandparent tier of the context
    override_timeout(ctx)
    client = get_client(**ctx.parent.parent.params)
    # Get a master-list of indices
    indices = get_indices(client)
    logger.debug("Full list of indices: {0}".format(indices))
    if index and not ctx.obj['filters']:
        working_list = []
    else:
        if indices:
            working_list = indices
        else:
            click.echo(click.style('ERROR. No indices found in Elasticsearch.', fg='red', bold=True))
            sys.exit(1)

    if all_indices:
        working_list = indices
        logger.info('Matching all indices. Ignoring flags other than --exclude.')

    logger.debug('All filters: {0}'.format(ctx.obj['filters']))

    for f in ctx.obj['filters']:
        if all_indices and not 'exclude' in f:
            continue
        logger.debug('Filter: {0}'.format(f))
        working_list = apply_filter(working_list, **f)

    if ctx.parent.info_name == "delete": # Protect against accidental delete
        logger.info("Pruning Kibana-related indices to prevent accidental deletion.")
        working_list = prune_kibana(working_list)

    # If there are manually added indices, we will add them here
    working_list.extend(in_list(index, indices))

    if working_list and ctx.parent.info_name == 'delete':
        # If filter by disk space, filter the working_list by space:
        if ctx.parent.params['disk_space']:
            working_list = filter_by_space(
                                client, working_list,
                                disk_space=ctx.parent.params['disk_space'],
                                reverse=ctx.parent.params['reverse']
                           )

    if working_list:
        # Make a sorted, unique list of indices
        working_list = sorted(list(set(working_list)))
        logger.debug('ACTION: {0}. INDICES: {1}'.format(ctx.parent.info_name, working_list))

        # Do action here!!! Don't forget to account for DRY_RUN!!!
        if ctx.parent.info_name == 'show':
            show(working_list)
        else:
            if ctx.parent.parent.params['dry_run']:
                show_dry_run(working_list, ctx.parent.info_name)
            else:
                # The snapshot command should get the full list, otherwise
                # the index list may need to be segmented.
                if len(to_csv(working_list)) > 3072 and not ctx.parent.info_name == 'snapshot':
                    logger.warn('Very large list of indices.  Breaking it up into smaller chunks.')
                    index_lists = chunk_index_list(working_list)
                    success = True
                    for l in index_lists:
                        retval = do_command(client, ctx.parent.info_name, l, ctx.parent.params)
                        if not retval:
                            success = False
                    exit_msg(success)
                else:
                    retval = do_command(client, ctx.parent.info_name, working_list, ctx.parent.params)
                    exit_msg(retval)

    else:
        logger.warn('No indices matched provided args: {0}'.format(ctx.params))
        click.echo(click.style('No indices matched provided args.', fg='red', bold=True))
        sys.exit(99)
