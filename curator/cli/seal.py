import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('seal')
@click.pass_context
def seal(ctx):
    """
    Seal indices
    (Synced flush: ES 1.6.0+ only)
    """
seal.add_command(indices)
