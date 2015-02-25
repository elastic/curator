import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('bloom')
@click.option('--delay', type=int, default=0, show_default=True, expose_value=True,
            help='Number of seconds to delay after disabling bloom filter cache of an index')
@click.pass_context
def bloom(ctx, delay):
    """Disable bloom filter cache"""
    ctx.obj["timeout_override"] = True
    logging.debug("ACTION: Disable bloom filter cache")
    if delay > 0:
        logging.debug("CONFIGURATION: Add a {0} second delay between iterations".format(delay))
bloom.add_command(indices)
