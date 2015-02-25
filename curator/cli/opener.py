import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('open')
@click.pass_context
def _open(ctx):
    """Open indices"""
    logging.debug("ACTION: Open indices")
_open.add_command(indices)
