import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'snapshot_prefix': 'curator-',
    'no_wait_for_completion': False,
    'ignore_unavailable': False,
    'include_global_state': True,
    'partial': False,
}

@cli.group('snapshot')
@click.option('--repository', help='Repository name.', expose_value=True, required=True)
@click.option('--snapshot-name', help='Override default name.', expose_value=True)
@click.option('--snapshot-prefix', help='Override default prefix.',
            expose_value=True, default=DEFAULT_ARGS['snapshot_prefix'])
@click.option('--no_wait_for_completion', is_flag=True, expose_value=True,
            help='Do not wait for snapshot to complete before returning.')
@click.option('--ignore_unavailable', is_flag=True, show_default=True, expose_value=True,
            help='Ignore unavailable shards/indices.', default=DEFAULT_ARGS['ignore_unavailable'])
@click.option('--include_global_state', is_flag=True, show_default=True,
            expose_value=True, help='Store cluster global state with snapshot.')
@click.option('--partial', is_flag=True, show_default=True, expose_value=True,
            help='Do not fail if primary shard is unavailable.')
@click.option('--request_timeout', type=int, default=218600, show_default=True,
            expose_value=True,
            help='Allow this many seconds before the transaction times out.')
@click.pass_context
def snapshot(ctx, repository, snapshot_name, snapshot_prefix, no_wait_for_completion, ignore_unavailable, include_global_state, partial):
    """Take snapshots of indices (Backup)"""
    ctx.obj["timeout_override"] = True
    logging.debug("ACTION: Take snapshots of indices (Backup)")
    if no_wait_for_completion:
        wait_for_completion = False
    else:
        wait_for_completion = True
snapshot.add_command(indices)
