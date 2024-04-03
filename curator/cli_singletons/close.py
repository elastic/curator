"""Close Singleton"""
import click
from curator.cli_singletons.object_class import CLIAction
from curator.cli_singletons.utils import validate_filter_json

@click.command()
@click.option('--search_pattern', type=str, default='_all', help='Elasticsearch Index Search Pattern')
@click.option('--delete_aliases', is_flag=True, help='Delete all aliases from indices to be closed')
@click.option('--skip_flush', is_flag=True, help='Skip flush phase for indices to be closed')
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
def close(
        ctx, search_pattern, delete_aliases, skip_flush, ignore_empty_list, allow_ilm_indices,
        filter_list):
    """
    Close Indices
    """
    manual_options = {
        'search_pattern': search_pattern,
        'skip_flush': skip_flush,
        'delete_aliases': delete_aliases,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = CLIAction(
        ctx.info_name, ctx.obj['configdict'], manual_options, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
