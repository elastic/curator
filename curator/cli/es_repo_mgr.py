import elasticsearch
import click
import re
import sys

from .utils import *
from ..api import *
from .. import __version__

import logging
logger = logging.getLogger(__name__)

try:
    from logging import NullHandler
except ImportError:
    from logging import Handler

    class NullHandler(Handler):
        def emit(self, record):
            pass

DEFAULT_ARGS = {
    'host': 'localhost',
    'url_prefix': '',
    'port': 9200,
    'http_auth': None,
    'use_ssl': False,
    'timeout': 300,
    'dry_run': False,
    'debug': False,
    'log_level': 'INFO',
    'logformat': 'default',
}

def show_repos(client):
    for repository in sorted(get_repository(client, '_all').keys()):
        print('{0}'.format(repository))
    sys.exit(0)

@click.command(short_help='Filesystem Repository')
@click.option('--repository', required=True, type=str, help='Repository name')
@click.option('--location', required=True, type=str,
            help='Shared file-system location. Must match remote path, & be accessible to all master & data nodes')
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
@click.pass_context
def fs(
    ctx, repository, location, compression, chunk_size,
    max_restore_bytes_per_sec, max_snapshot_bytes_per_sec):
    """
    Create a filesystem repository.
    """
    client = get_client(**ctx.parent.parent.params)
    success = False
    if create_repository(client, repo_type='fs', **ctx.params):
        success = verify_repository(client, repository)
    if not success:
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
@click.pass_context
def s3(
    ctx, repository, bucket, region, base_path, access_key, secret_key,
    compression, chunk_size, max_restore_bytes_per_sec,
    max_snapshot_bytes_per_sec):
    """
    Create an S3 repository.
    """
    client = get_client(**ctx.parent.parent.params)
    success = False
    if create_repository(client, repo_type='s3', **ctx.params):
        success = verify_repository(client, repository)
    if not success:
        sys.exit(1)

@click.group()
@click.option('--host', help='Elasticsearch host.', default=DEFAULT_ARGS['host'])
@click.option('--url_prefix', help='Elasticsearch http url prefix.', default=DEFAULT_ARGS['url_prefix'])
@click.option('--port', help='Elasticsearch port.', default=DEFAULT_ARGS['port'], type=int)
@click.option('--use_ssl', help='Connect to Elasticsearch through SSL.', is_flag=True, default=DEFAULT_ARGS['use_ssl'])
@click.option('--certificate', help='Path to certificate to use for SSL validation. (OPTIONAL)', type=str, default=None)
@click.option('--client-cert', help='Path to file containing SSL certificate for client auth. (OPTIONAL)', type=str, default=None)
@click.option('--client-key', help='Path to file containing SSL key for client auth. (OPTIONAL)', type=str, default=None)
@click.option('--ssl-no-validate', help='Do not validate server\'s SSL certificate', is_flag=True)
@click.option('--http_auth', help='Use Basic Authentication ex: user:pass', default=DEFAULT_ARGS['http_auth'])
@click.option('--timeout', help='Connection timeout in seconds.', default=DEFAULT_ARGS['timeout'], type=int)
@click.option('--master-only', is_flag=True, help='Only operate on elected master node.')
@click.option('--debug', is_flag=True, help='Debug mode', default=DEFAULT_ARGS['debug'])
@click.option('--loglevel', help='Log level', default=DEFAULT_ARGS['log_level'])
@click.option('--logfile', help='log file')
@click.option('--logformat', help='Log output format [default|logstash].', default=DEFAULT_ARGS['logformat'])
@click.version_option(version=__version__)
@click.pass_context
def repomgrcli(ctx, host, url_prefix, port, use_ssl, certificate, client_cert, client_key, ssl_no_validate, http_auth, timeout, master_only, debug, loglevel, logfile, logformat):
    """Repository manager for Elasticsearch Curator.
    """

    # Setup logging
    if debug:
        numeric_log_level = logging.DEBUG or loglevel.upper() == 'DEBUG'
        format_string = '%(asctime)s %(levelname)-9s %(name)22s %(funcName)22s:%(lineno)-4d %(message)s'
    else:
        numeric_log_level = getattr(logging, loglevel.upper(), None)
        format_string = '%(asctime)s %(levelname)-9s %(message)s'
        if not isinstance(numeric_log_level, int):
            raise ValueError('Invalid log level: {0}'.format(loglevel))

    handler = logging.StreamHandler(
        open(logfile, 'a') if logfile else sys.stdout)
    if logformat == 'logstash':
        handler.setFormatter(LogstashFormatter())
    else:
        handler.setFormatter(logging.Formatter(format_string))
    logging.root.addHandler(handler)
    logging.root.setLevel(numeric_log_level)

    # Filter out logging from Elasticsearch and associated modules by default
    if not debug:
        for handler in logging.root.handlers:
            handler.addFilter(
                Whitelist(
                    'root', '__main__', 'curator', 'curator.curator',
                    'curator.api', 'curator.cli', 'curator.api',
                    'curator.cli'
                )
            )

    # Setting up NullHandler to handle nested elasticsearch.trace Logger
    # instance in elasticsearch python client
    logging.getLogger('elasticsearch.trace').addHandler(NullHandler())

@repomgrcli.group('create')
@click.pass_context
def _create(ctx):
    """Create an Elasticsearch repository"""
_create.add_command(fs)
_create.add_command(s3)

@repomgrcli.command('show')
@click.pass_context
def show(ctx):
    """
    Show all repositories
    """
    client = get_client(**ctx.parent.params)
    show_repos(client)

@repomgrcli.command('delete')
@click.option('--repository', required=True, help='Repository name', type=str)
@click.option('--yes', is_flag=True, callback=delete_callback, expose_value=False,
                prompt='Are you sure you want to delete the repository?')
@click.pass_context
def _delete(ctx, repository):
    """Delete an Elasticsearch repository"""
    client = get_client(**ctx.parent.params)
    try:
        logger.info('Deleting repository {0}...'.format(repository))
        client.snapshot.delete_repository(repository=repository)
        sys.exit(0)
    except elasticsearch.NotFoundError:
        logger.error("Unable to delete repository: {0}  Not Found.".format(repository))
        sys.exit(1)
