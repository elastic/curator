import click
from .indices import *

import logging
logger = logging.getLogger(__name__)

@cli.group('alias')
@click.option('--name', required=True, help="Alias name", type=str)
@click.option('--remove', is_flag=True, show_default=True, expose_value=True,
            help='Remove from alias rather than add.')
@click.pass_context
def alias(ctx, name, remove):
    """Index Aliasing"""
    logging.debug("ACTION: Alias indices")
    if remove:
        logging.debug("CONFIGURATION: Remove indices from {0}".format(name))
    else:
        logging.debug("CONFIGURATION: Add indices to {0}".format(name))
alias.add_command(indices)
