import click
from .index_selection import *
from .snapshot_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('delete')
@click.option('--disk-space', type=float, expose_value=True,
            help='Delete indices beyond DISK_SPACE gigabytes.')
@click.option('--reverse', type=bool, default=True, expose_value=True,
            show_default=True, is_eager=True,
            help='Only valid with --disk-space. Affects sort order of the indices.  True means reverse-alphabetical (if dates are involved, older is deleted first).')
@click.option('--position', type=int, default=0, expose_value=True,
            help='Specify character position in index name to start sorting from. Can be negative to count back from end of name.')
@click.pass_context
def delete(ctx, disk_space, reverse, position):
    """Delete indices or snapshots"""
delete.add_command(indices)
delete.add_command(snapshots)
