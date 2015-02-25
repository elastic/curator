import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('replicas')
@click.option('--count', type=int, default=1, show_default=True, expose_value=True,
            help='Number of replicas the indices should have.', required=True)
@click.pass_context
def replicas(ctx, count):
    """Replica Count Per-shard"""
    logging.debug("ACTION: Update Replica Count Per-shard to {0}".format(count))
replicas.add_command(indices)
