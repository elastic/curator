import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('close')
@click.pass_context
def close(ctx):
    """Close indices"""
    logging.debug("ACTION: Close indices")
close.add_command(indices)
