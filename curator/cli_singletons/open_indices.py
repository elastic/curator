"""Open (closed) Index Singleton"""
import click
from curator.cli_singletons.object_class import CLIAction
from curator.cli_singletons.utils import validate_filter_json

@click.command(name='open')
@click.option('--search_pattern', type=str, default='_all', help='Elasticsearch Index Search Pattern')
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
def open_indices(ctx, search_pattern, ignore_empty_list, allow_ilm_indices, filter_list):
    """
    Open Indices
    """
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = CLIAction(
        ctx.info_name,
        ctx.obj['configdict'],
        {'search_pattern': search_pattern, 'allow_ilm_indices':allow_ilm_indices},
        filter_list,
        ignore_empty_list
    )
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
