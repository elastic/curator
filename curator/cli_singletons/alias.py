"""Alias Singleton"""
import logging
import click
from curator.cli_singletons.object_class import CLIAction
from curator.cli_singletons.utils import get_width, json_to_dict, validate_filter_json

@click.command(context_settings=get_width())
@click.option('--name', type=str, help='Alias name', required=True)
@click.option(
    '--add',
    callback=validate_filter_json,
    help='JSON array of filters selecting indices to ADD to alias',
    default=None
)
@click.option(
    '--remove',
    callback=validate_filter_json,
    help='JSON array of filters selecting indices to REMOVE from alias',
    default=None
)
@click.option(
    '--warn_if_no_indices',
    is_flag=True,
    help='Do not raise exception if there are no actionable indices in add/remove'
)
@click.option(
    '--extra_settings',
    help='JSON version of extra_settings (see documentation)',
    callback=json_to_dict
)
@click.option(
    '--allow_ilm_indices/--no-allow_ilm_indices',
    help='Allow Curator to operate on Index Lifecycle Management monitored indices.',
    default=False,
    show_default=True
)
@click.pass_context
def alias(ctx, name, add, remove, warn_if_no_indices, extra_settings, allow_ilm_indices):
    """
    Add/Remove Indices to/from Alias
    """
    logger = logging.getLogger('cli.singleton.alias')
    manual_options = {
        'name': name,
        'extra_settings': extra_settings,
        'allow_ilm_indices': allow_ilm_indices,
    }
    logger.debug('manual_options %s', manual_options)
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    ignore_empty_list = warn_if_no_indices
    logger.debug('ctx.info_name %s', ctx.info_name)
    logger.debug('ignore_empty_list %s', ignore_empty_list)
    logger.debug('add %s', add)
    logger.debug('remove %s', remove)
    logger.debug('warn_if_no_indices %s', warn_if_no_indices)
    logger.debug("ctx.obj['dry_run'] %s", ctx.obj['dry_run'])
    action = CLIAction(
        ctx.info_name,
        ctx.obj['config'],
        manual_options,
        [], # filter_list is empty in our case
        ignore_empty_list,
        add=add, remove=remove, warn_if_no_indices=warn_if_no_indices, # alias specific kwargs
    )
    logger.debug('We did not get here, did we?')
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
