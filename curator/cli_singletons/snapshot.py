"""Snapshot Singleton"""
import click
from curator.cli_singletons.object_class import CLIAction
from curator.cli_singletons.utils import validate_filter_json

# pylint: disable=line-too-long
@click.command()
@click.option('--search_pattern', type=str, default='_all', help='Elasticsearch Index Search Pattern')
@click.option('--repository', type=str, required=True, help='Snapshot repository')
@click.option('--name', type=str, help='Snapshot name', show_default=True, default='curator-%Y%m%d%H%M%S')
@click.option('--ignore_unavailable', is_flag=True, show_default=True, help='Ignore unavailable shards/indices.')
@click.option('--include_global_state', is_flag=True, show_default=True, help='Store cluster global state with snapshot.')
@click.option('--partial', is_flag=True, show_default=True, help='Do not fail if primary shard is unavailable.')
@click.option('--wait_for_completion/--no-wait_for_completion', default=True, show_default=True, help='Wait for the snapshot to complete')
@click.option('--wait_interval', default=9, type=int, help='Seconds to wait between completion checks.')
@click.option('--max_wait', default=-1, type=int, help='Maximum number of seconds to wait_for_completion')
@click.option('--skip_repo_fs_check', is_flag=True, show_default=True, help='Skip repository filesystem access validation.')
@click.option('--ignore_empty_list', is_flag=True, help='Do not raise exception if there are no actionable indices')
@click.option('--allow_ilm_indices/--no-allow_ilm_indices', help='Allow Curator to operate on Index Lifecycle Management monitored indices.', default=False, show_default=True)
@click.option('--filter_list', callback=validate_filter_json, help='JSON array of filters selecting indices to act on.', required=True)
@click.pass_context
def snapshot(
        ctx, search_pattern, repository, name, ignore_unavailable, include_global_state, partial,
        skip_repo_fs_check, wait_for_completion, wait_interval, max_wait, ignore_empty_list,
        allow_ilm_indices, filter_list
    ):
    """
    Snapshot Indices
    """
    manual_options = {
        'search_pattern': search_pattern,
        'name': name,
        'repository': repository,
        'ignore_unavailable': ignore_unavailable,
        'include_global_state': include_global_state,
        'partial': partial,
        'skip_repo_fs_check': skip_repo_fs_check,
        'wait_for_completion': wait_for_completion,
        'max_wait': max_wait,
        'wait_interval': wait_interval,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = CLIAction(ctx.info_name, ctx.obj['configdict'], manual_options, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
