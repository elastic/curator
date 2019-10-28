"""Change Replica Count Singleton"""
import click
from curator.cli_singletons.object_class import cli_action
from curator.cli_singletons.utils import get_width, validate_filter_json

@click.command(context_settings=get_width())
@click.option('--count', type=int, required=True, help='Number of replicas (max 10)')
@click.option(
    '--wait_for_completion/--no-wait_for_completion',
    default=False,
    help='Wait for replication to complete',
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
def replicas(ctx, count, wait_for_completion, ignore_empty_list, allow_ilm_indices, filter_list):
    """
    Change Replica Count
    """
    manual_options = {
        'count': count,
        'wait_for_completion': wait_for_completion,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(
        ctx.info_name, ctx.obj['config']['client'], manual_options, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
