import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('bloom')
@click.option('--delay', type=int, default=0, show_default=True, expose_value=True,
            help='Pause *n* seconds after disabling bloom filter cache of an index')
@click.option('--request_timeout', type=int, default=218600, show_default=True,
            expose_value=True,
            help='Allow this many seconds before the transaction times out.')
@click.pass_context
def bloom(ctx, delay):
    """Disable bloom filter cache"""
bloom.add_command(indices)
