import click
from curator.cli_singletons.object_class import cli_action
from curator.cli_singletons.utils import get_width, validate_filter_json

#### Indices ####
@click.command(context_settings=get_width())
@click.option('--ignore_empty_list', is_flag=True, help='Do not raise exception if there are no actionable indices')
@click.option('--allow_ilm_indices/--no-allow_ilm_indices', help='Allow Curator to operate on Index Lifecycle Management monitored indices.', default=False, show_default=True)
@click.option('--filter_list', callback=validate_filter_json, help='JSON array of filters selecting indices to act on.', required=True)
@click.pass_context
def delete_indices(ctx, ignore_empty_list, allow_ilm_indices, filter_list):
    """
    Delete Indices
    """
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(ctx.info_name, ctx.obj['config']['client'], {'allow_ilm_indices':allow_ilm_indices}, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])

#### Snapshots ####
@click.command(context_settings=get_width())
@click.option('--repository', type=str, required=True, help='Snapshot repository name')
@click.option('--retry_count', type=int, help='Number of times to retry (max 3)')
@click.option('--retry_interval', type=int, help='Time in seconds between retries')
@click.option('--ignore_empty_list', is_flag=True, help='Do not raise exception if there are no actionable snapshots')
@click.option('--allow_ilm_indices/--no-allow_ilm_indices', help='Allow Curator to operate on Index Lifecycle Management monitored indices.', default=False, show_default=True)
@click.option('--filter_list', callback=validate_filter_json, help='JSON array of filters selecting snapshots to act on.', required=True)
@click.pass_context
def delete_snapshots(ctx, repository, retry_count, retry_interval, ignore_empty_list, allow_ilm_indices, filter_list):
    """
    Delete Snapshots
    """
    manual_options = {
        'retry_count': retry_count, 
        'retry_interval': retry_interval,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(ctx.info_name, ctx.obj['config']['client'], manual_options, filter_list, ignore_empty_list, repository=repository)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])