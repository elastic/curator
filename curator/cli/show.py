import click
from .index_selection import *
from .snapshot_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('show')
@click.pass_context
def show(ctx):
    """Show indices or snapshots"""
show.add_command(indices)
show.add_command(snapshots)
