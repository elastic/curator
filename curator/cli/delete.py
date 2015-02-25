import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('delete')
@click.option('--disk-space', type=float, expose_value=True,
            help='Delete indices beyond DISK_SPACE gigabytes.')
@click.pass_context
def delete(ctx, disk_space):
    """Delete indices or snapshots"""
    logging.debug("ACTION: Delete indices")
    if disk_space:
        logging.debug("CONFIGURATION: Delete by space")
    else:
        logging.debug("CONFIGURATION: Delete by filter")
delete.add_command(indices)
delete.add_command(snapshots)
