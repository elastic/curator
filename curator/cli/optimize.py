import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('optimize')
@click.option('--delay', type=int, default=0, show_default=True, expose_value=True,
            help='Pause *n* seconds after optimizing an index.')
@click.option('--max_num_segments', type=int, default=2, show_default=True,
            expose_value=True,
            help='Merge to this number of segments per shard.')
@click.pass_context
def optimize(ctx, delay):
    """Optimize Indices"""
    ctx.obj["timeout_override"] = True
    logging.debug("ACTION: Optimize Indices")
    if delay > 0:
        logging.debug("CONFIGURATION: Add a {0} second delay between iterations".format(delay))
optimize.add_command(indices)
