import click
from curator.cli_singletons.object_class import cli_action
from curator.cli_singletons.utils import get_width, validate_filter_json

@click.command(name='open', context_settings=get_width())
@click.option('--ignore_empty_list', is_flag=True, help='Do not raise exception if there are no actionable indices')
@click.option('--filter_list', callback=validate_filter_json, help='JSON array of filters selecting indices to act on.', required=True)
@click.pass_context
def open_indices(ctx, ignore_empty_list, filter_list):
    """
    Open Indices
    """
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(ctx.info_name, ctx.obj['config']['client'], {}, filter_list, ignore_empty_list)
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
