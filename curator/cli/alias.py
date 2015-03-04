import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('alias')
@click.option('--name', help="Alias name", type=str)
@click.option('--remove', is_flag=True, show_default=True, expose_value=True,
            help='Remove from alias rather than add.')
@click.pass_context
def alias(ctx, name, remove):
    """Index Aliasing"""
    if not name:
        click.echo('{0}'.format(ctx.get_help()))
        click.echo(click.style('Missing required parameter --name', fg='red', bold=True))
        sys.exit(1)
alias.add_command(indices)
