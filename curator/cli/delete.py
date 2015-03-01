import click
from .index_selection import *
from .snapshot_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('delete')
@click.option('--disk-space', type=float, expose_value=True,
            help='Delete indices beyond DISK_SPACE gigabytes.')
@click.pass_context
def delete(ctx, disk_space):
    """Delete indices or snapshots"""
delete.add_command(indices)
delete.add_command(snapshots)
