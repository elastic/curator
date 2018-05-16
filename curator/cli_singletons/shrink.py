import click
from curator.cli_singletons.object_class import cli_action
from curator.cli_singletons.utils import get_width, json_to_dict, validate_filter_json

@click.command(context_settings=get_width())
@click.option('--shrink_node', default='DETERMINISTIC', type=str, help='Named node, or DETERMINISTIC', show_default=True)
@click.option('--node_filters', help='JSON version of node_filters (see documentation)', callback=json_to_dict)
@click.option('--number_of_shards', default=1, type=int, help='Shrink to this many shards per index')
@click.option('--number_of_replicas', default=1, type=int, help='Number of replicas for the target index', show_default=True)
@click.option('--shrink_prefix', type=str, help='Prefix for the target index name')
@click.option('--shrink_suffix', default='-shrink', type=str, help='Suffix for the target index name', show_default=True)
@click.option('--copy_aliases', is_flag=True, help='Copy each source index aliases to target index')
@click.option('--delete_after/--no-delete_after', default=True, help='Delete source index after shrink', show_default=True)
@click.option('--post_allocation', help='JSON version of post_allocation (see documentation)', callback=json_to_dict)
@click.option('--extra_settings', help='JSON version of extra_settings (see documentation)', callback=json_to_dict)
@click.option('--wait_for_active_shards', default=1, type=int, help='Wait for number of active shards before continuing')
@click.option('--wait_for_rebalance/--no-wait_for_rebalance', default=True, help='Wait for rebalance to complete')
@click.option('--wait_for_completion/--no-wait_for_completion', default=True, help='Wait for the shrink to complete')
@click.option('--wait_interval', default=9, type=int, help='Seconds to wait between completion checks.')
@click.option('--max_wait', default=-1, type=int, help='Maximum number of seconds to wait_for_completion')
@click.option('--ignore_empty_list', is_flag=True, help='Do not raise exception if there are no actionable indices')
@click.option('--allow_ilm_indices/--no-allow_ilm_indices', help='Allow Curator to operate on Index Lifecycle Management monitored indices.', default=False, show_default=True)
@click.option('--filter_list', callback=validate_filter_json, help='JSON array of filters selecting indices to act on.', required=True)
@click.pass_context
def shrink(ctx, shrink_node, node_filters, number_of_shards, number_of_replicas, shrink_prefix,
    shrink_suffix, copy_aliases, delete_after, post_allocation, extra_settings, wait_for_active_shards,
    wait_for_rebalance, wait_for_completion, wait_interval, max_wait, ignore_empty_list, allow_ilm_indices, filter_list):
    """
    Shrink Indices to --number_of_shards
    """
    manual_options = {
        'shrink_node': shrink_node,
        'node_filters': node_filters,
        'number_of_shards': number_of_shards,
        'number_of_replicas': number_of_replicas,
        'shrink_prefix': shrink_prefix,
        'shrink_suffix': shrink_suffix,
        'copy_aliases': copy_aliases,
        'delete_after': delete_after,
        'post_allocation': post_allocation,
        'extra_settings': extra_settings,
        'wait_for_active_shards': wait_for_active_shards,
        'wait_for_rebalance': wait_for_rebalance,
        'wait_for_completion': wait_for_completion,
        'wait_interval': wait_interval,
        'max_wait': max_wait,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(ctx.info_name, ctx.obj['config']['client'], manual_options, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])