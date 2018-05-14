import click
import os
from curator.defaults import settings
from curator.config_utils import test_config, set_logging
from curator.utils import test_client_options
from curator.cli_singletons.utils import config_override, false_to_none, get_width
from curator.cli_singletons.alias import alias
from curator.cli_singletons.allocation import allocation
from curator.cli_singletons.close import close
from curator.cli_singletons.delete import delete_indices, delete_snapshots
from curator.cli_singletons.forcemerge import forcemerge
from curator.cli_singletons.open_indices import open_indices
from curator.cli_singletons.replicas import replicas
from curator.cli_singletons.restore import restore
from curator.cli_singletons.rollover import rollover
from curator.cli_singletons.snapshot import snapshot
from curator.cli_singletons.shrink import shrink
from curator.cli_singletons.show import show_indices, show_snapshots
from curator._version import __version__

import logging
logger = logging.getLogger(__name__)

@click.group(context_settings=get_width())
@click.option('--config', help='Path to configuration file. Default: ~/.curator/curator.yml', type=click.Path(), default=settings.config_file())
@click.option('--host', help='Elasticsearch host.')
@click.option('--url_prefix', help='Elasticsearch http url prefix.')
@click.option('--port', help='Elasticsearch port.')
@click.option('--use_ssl', is_flag=True, callback=false_to_none, help='Connect to Elasticsearch through SSL.')
@click.option('--certificate', help='Path to certificate to use for SSL validation.')
@click.option('--client-cert', help='Path to file containing SSL certificate for client auth.', type=str)
@click.option('--client-key', help='Path to file containing SSL key for client auth.', type=str)
@click.option('--ssl-no-validate', is_flag=True, callback=false_to_none, help='Do not validate SSL certificate')
@click.option('--http_auth', help='Use Basic Authentication ex: user:pass')
@click.option('--timeout', help='Connection timeout in seconds.', type=int)
@click.option('--master-only', is_flag=True, callback=false_to_none, help='Only operate on elected master node.')
@click.option('--dry-run', is_flag=True, help='Do not perform any changes.')
@click.option('--loglevel', help='Log level')
@click.option('--logfile', help='log file')
@click.option('--logformat', help='Log output format [default|logstash|json].')
@click.version_option(version=__version__)
@click.pass_context
def cli(
    ctx, config, host, url_prefix, port, use_ssl, certificate, client_cert,
    client_key, ssl_no_validate, http_auth, timeout, master_only, dry_run,
    loglevel, logfile, logformat):
    if os.path.isfile(config):
        initial_config = test_config(config)
    else:
        initial_config = None
    configuration = config_override(ctx, initial_config)
    set_logging(configuration['logging'])
    test_client_options(configuration['client'])
    ctx.obj['config'] = configuration
    ctx.obj['dry_run'] = dry_run
# Add the subcommands
cli.add_command(alias)
cli.add_command(allocation)
cli.add_command(close)
cli.add_command(delete_indices)
cli.add_command(delete_snapshots)
cli.add_command(forcemerge)
cli.add_command(open_indices)
cli.add_command(replicas)
cli.add_command(snapshot)
cli.add_command(restore)
cli.add_command(rollover)
cli.add_command(shrink)
cli.add_command(show_indices)
cli.add_command(show_snapshots)
