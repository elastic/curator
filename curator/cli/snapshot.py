import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'snapshot_prefix': 'curator-',
    'wait_for_completion': True,
    'include_global_state': True,
}

@cli.group('snapshot')
@click.option('--repository', help='Repository name.', expose_value=True)
@click.option('--name', help='Override default name.', expose_value=True)
@click.option('--prefix', help='Override default prefix.',
            expose_value=True, default=DEFAULT_ARGS['snapshot_prefix'])
@click.option('--wait_for_completion', type=bool, show_default=True,
            default=DEFAULT_ARGS['wait_for_completion'], expose_value=True,
            help='Wait for snapshot to complete before returning.')
@click.option('--ignore_unavailable', is_flag=True, expose_value=True,
            help='Ignore unavailable shards/indices.')
@click.option('--include_global_state', type=bool, show_default=True,
            default=DEFAULT_ARGS['include_global_state'], expose_value=True, 
            help='Store cluster global state with snapshot.')
@click.option('--partial', is_flag=True, expose_value=True,
            help='Do not fail if primary shard is unavailable.')
@click.option('--request_timeout', type=int, default=21600, show_default=True,
            expose_value=True,
            help='Allow this many seconds before the transaction times out.')
@click.pass_context
def snapshot(
        ctx, repository, name, prefix, wait_for_completion, ignore_unavailable,
        include_global_state, partial, request_timeout
    ):
    """Take snapshots of indices (Backup)"""
    if not repository:
        click.echo('{0}'.format(ctx.get_help()))
        click.echo(click.style('Missing required parameter --repository', fg='red', bold=True))
        sys.exit(1)
snapshot.add_command(indices)
