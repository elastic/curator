import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('close')
@click.pass_context
def close(ctx):
    """Close indices"""
close.add_command(indices)
