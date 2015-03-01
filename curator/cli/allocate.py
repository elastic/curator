import elasticsearch
import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('allocation')
@click.option('--rule', show_default=True, expose_value=True, type=str,
            help='Routing allocation rule to apply, e.g. tag=ssd')
@click.pass_context
def allocation(ctx, rule):
    """Index Allocation"""
    if not rule:
        click.echo('{0}'.format(ctx.get_help()))
        click.echo(click.style('Missing required parameter --rule', fg='red', bold=True))
        sys.exit(1)
allocation.add_command(indices)
