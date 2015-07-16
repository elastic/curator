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

    # Base and client args are in the grandparent tier of the context
    logger.debug("Params: {0}".format(ctx.parent.parent.params))

    # Set master_timeout to match 'timeout' if less than or equal to 300 seconds,
    # otherwise set to 300.  Also, set this before overriding the timeout.
    master_timeout = ctx.parent.parent.params['timeout'] if ctx.parent.parent.params['timeout'] <= 300 else 300
    master_timeout = master_timeout * 1000 # This value is in milliseconds, at least in Python.
    # If this is in error, it may be somewhere in the Python module or urllib3
    # The Elasticsearch output was timing out deletes because they failed to
    # complete within 30ms (until I multiplied by 1000)
    override_timeout(ctx)

    # Get the client
    client = get_client(**ctx.parent.parent.params)

    # Get a master-list of indices
    indices = get_indices(client)
    logger.debug("Full list of indices: {0}".format(indices))

    # Build index list
    if index and not ctx.obj['filters']:
        working_list = []
    else:
        if indices:
            working_list = indices
        else:
            click.echo(click.style('ERROR. No indices found in Elasticsearch.', fg='red', bold=True))
            sys.exit(1)

    # Override any other flags if --all_indices specified
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
        ### Issue #348
        # I don't care about using only --timestring if it's a `show` or `dry_run`
        if timestring and not newer_than and not older_than \
          and not (ctx.parent.info_name == 'show') \
          and not ctx.parent.parent.params['dry_run']:
            click.echo(click.style('You are using --timestring without --older-than or --newer-than.', fg='yellow', bold=True))
            click.echo('This could result in actions being performed on all indices matching {0}'.format(timestring))
            click.echo(click.style('Press CTRL-C to exit Curator before the timer expires:', fg='red', bold=True))
            countdown(10)
        # Make a sorted, unique list of indices
        working_list = sorted(list(set(working_list)))
        logger.debug('ACTION: {0}. INDICES: {1}'.format(ctx.parent.info_name, working_list))

        # Do action here!!! Don't forget to account for DRY_RUN!!!
        if ctx.parent.info_name == 'show':
            logger.info('Matching indices:')
            show(client, working_list, type='indices')
        else:
            if ctx.parent.parent.params['dry_run']:
                show_dry_run(client, working_list, ctx.parent.info_name, type='indices')
            else:
                # The snapshot command should get the full list, otherwise
                # the index list may need to be segmented.
                if len(to_csv(working_list)) > 3072 and not ctx.parent.info_name == 'snapshot':
                    logger.warn('Very large list of indices.  Breaking it up into smaller chunks.')
                    index_lists = chunk_index_list(working_list)
                    success = True
                    for l in index_lists:
                        retval = do_command(client, ctx.parent.info_name, l, ctx.parent.params, master_timeout)
                        if not retval:
                            success = False
                        time.sleep(2) # Pause in between iterations
                    exit_msg(success)
                else:
                    retval = do_command(client, ctx.parent.info_name, working_list, ctx.parent.params, master_timeout)
                    exit_msg(retval)

    else:
        logger.warn('No indices matched provided args: {0}'.format(ctx.params))
        click.echo(click.style('No indices matched provided args.', fg='red', bold=True))
        sys.exit(0)
