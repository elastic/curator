import elasticsearch
import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('allocation')
@click.option('--rule', show_default=True, expose_value=True, type=str,
            help='Routing allocation rule to apply, e.g. tag=ssd')
@click.option('--type', default='require', show_default=True, expose_value=True, type=str,
            help='Specify an allocation type, include, exclude or require')
@click.pass_context
def allocation(ctx, rule, type):
    """Index Allocation"""
    if not rule:
        click.echo('{0}'.format(ctx.get_help()))
        click.echo(click.style('Missing required parameter --rule', fg='red', bold=True))
        sys.exit(1)

    if type not in ['require', 'include', 'exclude']:
        click.echo(click.style('--type can only be one of: require, include  exclude', fg='red', bold=True))
        sys.exit(1)

allocation.add_command(indices)
