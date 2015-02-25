import elasticsearch
import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('allocation')
@click.option('--rule', show_default=True, expose_value=True, type=str, required=True,
            help='Routing allocation rule to apply, e.g. tag=ssd')
@click.pass_context
def allocation(ctx, rule):
    """Index Allocation"""
    logging.debug("ACTION: Index Allocation")
    logging.debug("CONFIGURATION: rule = {0}".format(rule))
allocation.add_command(indices)
