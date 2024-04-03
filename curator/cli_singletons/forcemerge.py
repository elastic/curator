"""ForceMerge Singleton"""
import click
from curator.cli_singletons.object_class import CLIAction
from curator.cli_singletons.utils import validate_filter_json

@click.command()
@click.option('--search_pattern', type=str, default='_all', help='Elasticsearch Index Search Pattern')
@click.option(
    '--max_num_segments',
    type=int,
    required=True,
    help='Maximum number of segments per shard (minimum of 1)'
)
@click.option(
    '--delay',
    type=float,
    help='Time in seconds to delay between operations. Default 0. Maximum 3600'
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
    required=True)
@click.pass_context
def forcemerge(
        ctx, search_pattern, max_num_segments, delay, ignore_empty_list, allow_ilm_indices,
        filter_list):
    """
    forceMerge Indices (reduce segment count)
    """
    manual_options = {
        'search_pattern': search_pattern,
        'max_num_segments': max_num_segments,
        'delay': delay,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = CLIAction(
        ctx.info_name, ctx.obj['configdict'], manual_options, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
