import elasticsearch
import click
import re
import sys
import logging
from .defaults import settings
from .exceptions import *
from .config_utils import process_config
from .utils import *
from ._version import __version__

logger = logging.getLogger('curator.repomgrcli')

def delete_callback(ctx, param, value):
    if not value:
        ctx.abort()

def show_repos(client):
    for repository in sorted(get_repository(client, '_all').keys()):
        print('{0}'.format(repository))
    sys.exit(0)

@click.command(short_help='Filesystem Repository')
@click.option('--repository', required=True, type=str, help='Repository name')
@click.option(
    '--location',
    required=True,
    type=str,
    help=(
        'Shared file-system location. '
        'Must match remote path, & be accessible to all master & data nodes'
    )
)
@click.option('--compression', type=bool, default=True, show_default=True,
            help='Enable/Disable metadata compression.')
@click.option('--chunk_size', type=str,
            help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--max_restore_bytes_per_sec', type=str, default='20mb',
            show_default=True,
            help='Throttles per node restore rate (per second).')
@click.option('--max_snapshot_bytes_per_sec', type=str, default='20mb',
            show_default=True,
            help='Throttles per node snapshot rate (per second).')
@click.option('--skip_repo_fs_check', type=bool, default=False, show_default=True,
            help='Skip repository verification after creation')
@click.pass_context
def fs(
    ctx, repository, location, compression, chunk_size,
    max_restore_bytes_per_sec, max_snapshot_bytes_per_sec,
    skip_repo_fs_check):
    """
    Create a filesystem repository.
    """
    logger = logging.getLogger('curator.repomgrcli.fs')
    client = get_client(**ctx.obj['client_args'])
    try:
        create_repository(client, repo_type='fs', **ctx.params)
    except FailedExecution as e:
        logger.critical(e)
        sys.exit(1)


@click.command(short_help='S3 Repository')
@click.option('--repository', required=True, type=str, help='Repository name')
@click.option('--bucket', required=True, type=str, help='S3 bucket name')
@click.option('--region', type=str, help='S3 region. [US Standard]')
@click.option('--base_path', type=str, help='S3 base path. [root]')
@click.option('--access_key', type=str,
            help='S3 access key. [value of cloud.aws.access_key]')
@click.option('--secret_key', type=str,
            help='S3 secret key. [value of cloud.aws.secret_key]')
@click.option('--compression', type=bool, default=True, show_default=True,
            help='Enable/Disable metadata compression.')
@click.option('--chunk_size', type=str,
            help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--max_restore_bytes_per_sec', type=str, default='20mb',
            show_default=True,
            help='Throttles per node restore rate (per second).')
@click.option('--max_snapshot_bytes_per_sec', type=str, default='20mb',
            show_default=True,
            help='Throttles per node snapshot rate (per second).')
@click.option('--skip_repo_fs_check', type=bool, default=False, show_default=True,
            help='Skip repository verification after creation')
@click.pass_context
def s3(
    ctx, repository, bucket, region, base_path, access_key, secret_key,
    compression, chunk_size, max_restore_bytes_per_sec,
    max_snapshot_bytes_per_sec, skip_repo_fs_check):
    """
    Create an S3 repository.
    """
    logger = logging.getLogger('curator.repomgrcli.s3')
    client = get_client(**ctx.obj['client_args'])
    try:
        create_repository(client, repo_type='s3', **ctx.params)
    except FailedExecution as e:
        logger.critical(e)
        sys.exit(1)


@click.group()
@click.option(
    '--config',
    help="Path to configuration file. Default: ~/.curator/curator.yml",
    type=click.Path(exists=True), default=settings.config_file()
)
@click.pass_context
def repo_mgr_cli(ctx, config):
    """
    Repository manager for Elasticsearch Curator.
    """
    ctx.obj = {}
    ctx.obj['client_args'] = process_config(config)
    logger = logging.getLogger(__name__)
    logger.debug('Client and logging options validated.')

@repo_mgr_cli.group('create')
@click.pass_context
def _create(ctx):
    """Create an Elasticsearch repository"""
_create.add_command(fs)
_create.add_command(s3)

@repo_mgr_cli.command('show')
@click.pass_context
def show(ctx):
    """
    Show all repositories
    """
    client = get_client(**ctx.obj['client_args'])
    show_repos(client)

@repo_mgr_cli.command('delete')
@click.option('--repository', required=True, help='Repository name', type=str)
@click.option('--yes', is_flag=True, callback=delete_callback,
                expose_value=False,
                prompt='Are you sure you want to delete the repository?')
@click.pass_context
def _delete(ctx, repository):
    """Delete an Elasticsearch repository"""
    client = get_client(**ctx.obj['client_args'])
    try:
        logger.info('Deleting repository {0}...'.format(repository))
        client.snapshot.delete_repository(repository=repository)
    except elasticsearch.NotFoundError:
        logger.error(
            'Unable to delete repository: {0}  Not Found.'.format(repository))
        sys.exit(1)
