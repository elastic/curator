"""Allocation Singleton"""
import click
from curator.cli_singletons.object_class import CLIAction
from curator.cli_singletons.utils import validate_filter_json

@click.command()
@click.option('--search_pattern', type=str, default='_all', help='Elasticsearch Index Search Pattern')
@click.option('--key', type=str, required=True, help='Node identification tag')
@click.option('--value', type=str, default=None, help='Value associated with --key')
@click.option('--allocation_type', type=click.Choice(['require', 'include', 'exclude']))
@click.option(
    '--wait_for_completion/--no-wait_for_completion',
    default=False,
    help='Wait for the allocation to complete',
    show_default=True
)
@click.option(
    '--max_wait',
    default=-1,
    type=int,
    help='Maximum number of seconds to wait_for_completion',
    show_default=True
)
@click.option(
    '--wait_interval',
    default=9,
    type=int,
    help='Seconds to wait between completion checks.',
    show_default=True
)
@click.option(
    '--ignore_empty_list',
    is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--allow_ilm_indices/--no-allow_ilm_indices',
    help='Allow Curator to operate on Index Lifecycle Management monitored indices.',
    default=False,
    show_default=True
)
@click.option(
    '--filter_list',
    callback=validate_filter_json,
    help='JSON array of filters selecting indices to act on.',
    required=True
)
@click.pass_context
def allocation(
        ctx,
        search_pattern,
        key,
        value,
        allocation_type,
        wait_for_completion,
        max_wait,
        wait_interval,
        ignore_empty_list,
        allow_ilm_indices,
        filter_list
    ):
    """
    Shard Routing Allocation
    """
    manual_options = {
        'search_pattern': search_pattern,
        'key': key,
        'value': value,
        'allocation_type': allocation_type,
        'wait_for_completion': wait_for_completion,
        'max_wait': max_wait,
        'wait_interval': wait_interval,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = CLIAction(
        ctx.info_name, ctx.obj['configdict'], manual_options, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
