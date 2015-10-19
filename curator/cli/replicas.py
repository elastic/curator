import click
from .index_selection import *

import logging
logger = logging.getLogger(__name__)

@cli.group('replicas')
@click.option('--count', type=int, expose_value=True,
            help='Number of replicas the indices should have.')
@click.pass_context
def replicas(ctx, count):
    """Replica Count Per-shard"""
    if count == None: # Have to do this since 0 is valid
        msgout('{0}'.format(ctx.get_help()), quiet=ctx.parent.params['quiet'])
        logger.error('Missing required parameter --count')
        msgout('Missing required parameter --count', error=True, quiet=ctx.parent.params['quiet'])
        sys.exit(1)
replicas.add_command(indices)
