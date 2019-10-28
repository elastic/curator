"""Index Rollover Singleton"""
import click
from curator.cli_singletons.object_class import cli_action
from curator.cli_singletons.utils import get_width, json_to_dict, validate_filter_json

@click.command(context_settings=get_width())
@click.option('--name', type=str, help='Alias name', required=True)
@click.option('--max_age', type=str, help='max_age condition value (see documentation)')
@click.option('--max_docs', type=str, help='max_docs condition value (see documentation)')
@click.option('--max_size', type=str, help='max_size condition value (see documentation)')
@click.option(
    '--extra_settings',
    type=str,
    help='JSON version of extra_settings (see documentation)',
    callback=json_to_dict
)
@click.option(
    '--new_index',
    type=str,
    help='Optional new index name (see documentation)'
)
@click.option(
    '--wait_for_active_shards',
    type=int,
    default=1,
    show_default=True,
    help='Wait for number of shards to be active before returning'
)
@click.option(
    '--allow_ilm_indices/--no-allow_ilm_indices',
    help='Allow Curator to operate on Index Lifecycle Management monitored indices.',
    default=False,
    show_default=True
)
@click.pass_context
def rollover(
        ctx, name, max_age, max_docs, max_size, extra_settings, new_index, wait_for_active_shards,
        allow_ilm_indices
    ):
    """
    Rollover Index associated with Alias
    """
    conditions = {}
    for cond in ['max_age', 'max_docs', 'max_size']:
        if eval(cond) is not None:
            conditions[cond] = eval(cond)
    manual_options = {
        'name': name,
        'conditions': conditions,
        'allow_ilm_indices': allow_ilm_indices,
    }
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(
        ctx.info_name, ctx.obj['config']['client'], manual_options, [], True,
        extra_settings=extra_settings,
        new_index=new_index,
        wait_for_active_shards=wait_for_active_shards
    )
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
