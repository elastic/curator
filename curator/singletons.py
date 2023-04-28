"""CLI module for curator_cli"""
import click
from es_client.builder import ClientArgs, OtherArgs
from es_client.helpers.utils import check_config, get_yaml, prune_nones, verify_url_schema
from curator.defaults import settings
from curator.config_utils import check_logging_config, set_logging
from curator._version import __version__
from curator.cli_singletons import (
    alias, allocation, close, delete_indices, delete_snapshots, forcemerge, open_indices, replicas,
    restore, rollover, snapshot, shrink
)
from curator.cli_singletons.show import show_indices, show_snapshots
from curator.cli_singletons.utils import get_width

# pylint: disable=unused-argument, redefined-builtin
@click.group(context_settings=get_width())
@click.option('--config', help='Path to configuration file.', type=click.Path(exists=True), default=settings.config_file())
@click.option('--hosts', help='Elasticsearch URL to connect to', multiple=True)
@click.option('--cloud_id', help='Shorthand to connect to Elastic Cloud instance')
@click.option('--api_token', help='The base64 encoded API Key token', type=str)
@click.option('--id', help='API Key "id" value', type=str)
@click.option('--api_key', help='API Key "api_key" value', type=str)
@click.option('--username', help='Username used to create "basic_auth" tuple')
@click.option('--password', help='Password used to create "basic_auth" tuple')
@click.option('--bearer_auth', type=str)
@click.option('--opaque_id', type=str)
@click.option('--request_timeout', help='Request timeout in seconds', type=float)
@click.option('--http_compress', help='Enable HTTP compression', is_flag=True, default=None)
@click.option('--verify_certs', help='Verify SSL/TLS certificate(s)', is_flag=True, default=None)
@click.option('--ca_certs', help='Path to CA certificate file or directory')
@click.option('--client_cert', help='Path to client certificate file')
@click.option('--client_key', help='Path to client certificate key')
@click.option('--ssl_assert_hostname', help='Hostname or IP address to verify on the node\'s certificate.', type=str)
@click.option('--ssl_assert_fingerprint', help='SHA-256 fingerprint of the node\'s certificate. If this value is given then root-of-trust verification isn\'t done and only the node\'s certificate fingerprint is verified.', type=str)
@click.option('--ssl_version', help='Minimum acceptable TLS/SSL version', type=str)
@click.option('--master-only', help='Only run if the single host provided is the elected master', is_flag=True, default=None)
@click.option('--skip_version_test', help='Do not check the host version', is_flag=True, default=None)
@click.option('--dry-run', is_flag=True, help='Do not perform any changes.')
@click.option('--loglevel', help='Log level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
@click.option('--logfile', help='log file')
@click.option('--logformat', help='Log output format', type=click.Choice(['default', 'logstash', 'json', 'ecs']))
@click.version_option(version=__version__)
@click.pass_context
def cli(
    ctx, config, hosts, cloud_id, api_token, id, api_key, username, password, bearer_auth,
    opaque_id, request_timeout, http_compress, verify_certs, ca_certs, client_cert, client_key,
    ssl_assert_hostname, ssl_assert_fingerprint, ssl_version, master_only, skip_version_test,
    dry_run, loglevel, logfile, logformat
):
    """CLI input"""
    client_args = ClientArgs()
    other_args = OtherArgs()
    if config:
        from_yaml = get_yaml(config)
        raw_config = check_config(from_yaml)
        client_args.update_settings(raw_config['client'])
        other_args.update_settings(raw_config['other_settings'])

    # Check for log settings from config file
    init_logcfg = check_logging_config(from_yaml)

    # Override anything with options from the command-line
    if loglevel:
        init_logcfg['loglevel'] = loglevel
    if logfile:
        init_logcfg['logfile'] = logfile
    if logformat:
        init_logcfg['logformat'] = logformat

    # Now enable logging with the merged settings
    set_logging(check_logging_config({'logging': init_logcfg}))

    hostslist = []
    if hosts:
        for host in list(hosts):
            hostslist.append(verify_url_schema(host))
    else:
        hostslist = None

    cli_client = prune_nones({
        'hosts': hostslist,
        'cloud_id': cloud_id,
        'bearer_auth': bearer_auth,
        'opaque_id': opaque_id,
        'request_timeout': request_timeout,
        'http_compress': http_compress,
        'verify_certs': verify_certs,
        'ca_certs': ca_certs,
        'client_cert': client_cert,
        'client_key': client_key,
        'ssl_assert_hostname': ssl_assert_hostname,
        'ssl_assert_fingerprint': ssl_assert_fingerprint,
        'ssl_version': ssl_version
    })

    cli_other = prune_nones({
        'master_only': master_only,
        'skip_version_test': skip_version_test,
        'username': username,
        'password': password,
        'api_key': {
            'id': id,
            'api_key': api_key,
            'token': api_token,
        }
    })
    # Remove `api_key` root key if `id` and `api_key` and `token` are all None
    if id is None and api_key is None and api_token is None:
        del cli_other['api_key']

    # If hosts are in the config file, but cloud_id is specified at the command-line,
    # we need to remove the hosts parameter as cloud_id and hosts are mutually exclusive
    if cloud_id:
        click.echo('cloud_id provided at CLI, superseding any other configured hosts')
        client_args.hosts = None
        cli_client.pop('hosts', None)

    # Likewise, if hosts are provided at the command-line, but cloud_id was in the config file,
    # we need to remove the cloud_id parameter from the config file-based dictionary before merging
    if hosts:
        click.echo('hosts specified manually, superseding any other cloud_id or hosts')
        client_args.hosts = None
        client_args.cloud_id = None
        cli_client.pop('cloud_id', None)

    # Update the objects if we have settings after pruning None values
    if cli_client:
        client_args.update_settings(cli_client)
    if cli_other:
        other_args.update_settings(cli_other)

    # Build a "final_config" that reflects CLI args overriding anything from a config_file
    final_config = {
        'elasticsearch': {
            'client': prune_nones(client_args.asdict()),
            'other_settings': prune_nones(other_args.asdict())
        }
    }

    ctx.obj['config'] = final_config
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
