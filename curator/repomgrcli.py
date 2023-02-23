"""es_repo_mgr CLI"""
import sys
import logging
import pprint
import click
from elasticsearch8 import ApiError, NotFoundError
from es_client.builder import ClientArgs, OtherArgs, Builder
from es_client.helpers.utils import check_config, get_yaml, prune_nones, verify_url_schema
from curator.defaults import settings
from curator.config_utils import check_logging_config, set_logging
from curator.helpers.getters import get_repository
from curator._version import __version__
from curator.cli_singletons.utils import get_width

# pylint: disable=unused-argument
def delete_callback(ctx, param, value):
    """Callback if command ``delete`` called

    If the action is ``delete``, this is the :py:class:`click.Parameter` callback function if you
    used the ``--yes`` flag.

    :param ctx: The Click Context
    :param param: The parameter name
    :param value: The value

    :type ctx: :py:class:`~.click.Context`
    :type param: str
    :type value: str
    """
    # It complains if ``param`` isn't passed as a positional parameter, but this is a one-trick
    # pony callback. We don't need to know that you selected ``--yes`` as the parameter.
    if not value:
        ctx.abort()

def show_repos(client):
    """Show all repositories

    :param client: A client connection object
    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :rtype: None
    """
    for repository in sorted(get_repository(client, '*').keys()):
        click.echo(f'{repository}')
    sys.exit(0)

def get_client(ctx):
    """
    :param ctx: The :py:class:`click` Context
    :type ctx: :py:class:`~.click.Context`

    :returns: A client connection object
    :rtype: :py:class:`~.elasticsearch.Elasticsearch`
    """
    builder = Builder(configdict=ctx.obj['esconfig'])
    try:
        builder.connect()
    # pylint: disable=broad-except
    except Exception as exc:
        click.echo(f'Exception encountered: {exc}')
        sys.exit(1)
    return builder.client

# pylint: disable=broad-except
def create_repo(ctx, repo_name=None, repo_type=None, repo_settings=None, verify=False):
    """
    Call :py:meth:`~.elasticsearch.client.SnapshotClient.create_repository` to create a snapshot
    repository from the provided arguments

    :param ctx: The :py:class:`click` Context
    :param repo_name: The repository name
    :param repo_type: The repository name
    :param repo_settings: Settings to configure the repository
    :param verify: Whether to verify repository access

    :type ctx: :py:class:`~.click.Context`
    :type repo_name: str
    :type repo_type: str
    :type repo_settings: dict
    :type verify: bool
    :rtype: None
    """
    logger = logging.getLogger('curator.repomgrcli.create_repo')
    esclient = get_client(ctx)
    try:
        esclient.snapshot.create_repository(
            name=repo_name, settings=repo_settings, type=repo_type, verify=verify)
    except ApiError as exc:
        if exc.meta.status >= 500:
            logger.critical('Server-side error!')
            if exc.body['error']['type'] == 'repository_exception':
                logger.critical('Repository exception: %s', exc.body)
        click.echo(f'Repository exception: {exc.body}')
        sys.exit(1)
    except Exception as exc:
        logger.critical('Encountered exception %s', exc)
        sys.exit(1)

# pylint: disable=unused-argument, invalid-name, line-too-long
@click.command(short_help='Azure Repository')
@click.option('--name', required=True, type=str, help='Repository name')
@click.option('--client', default='default', show_default=True, type=str, help='Azure named client to use.')
@click.option('--container', default='elasticsearch-snapshots', show_default=True, type=str, help='Container name. You must create the Azure container before creating the repository.')
@click.option('--base_path', default='', show_default=True, type=str, help='Specifies the path within container to repository data. Defaults to empty (root directory).')
@click.option('--chunk_size', type=str, help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--compress/--no-compress', default=True, show_default=True, help='Enable/Disable metadata compression.')
@click.option('--max_restore_rate', type=str, default='20mb', show_default=True, help='Throttles per node restore rate (per second).')
@click.option('--max_snapshot_rate', type=str, default='20mb', show_default=True, help='Throttles per node snapshot rate (per second).')
@click.option('--readonly', is_flag=True, help='If set, the repository is read-only.')
@click.option('--location_mode', default='primary_only', type=click.Choice(['primary_only', 'secondary_only']), help='Note that if you set it to secondary_only, it will force readonly to true.')
@click.option('--verify', is_flag=True, help='Verify repository after creation.')
@click.pass_context
def azure(
        ctx, name, client, container, base_path, chunk_size, compress, max_restore_rate,
        max_snapshot_rate, readonly, location_mode, verify
    ):
    """Create an Azure repository."""
    azure_settings = {
        'client': client,
        'container': container,
        'base_path': base_path,
        'chunk_size': chunk_size,
        'compress': compress,
        'max_restore_bytes_per_sec': max_restore_rate,
        'max_snapshot_bytes_per_sec': max_snapshot_rate,
        'readonly': readonly,
        'location_mode': location_mode
    }
    create_repo(ctx, repo_name=name, repo_type='azure', repo_settings=azure_settings, verify=verify)

# pylint: disable=unused-argument, invalid-name, line-too-long
@click.command(short_help='Google Cloud Storage Repository')
@click.option('--name', required=True, type=str, help='Repository name')
@click.option('--bucket', required=True, type=str, help='The name of the bucket to be used for snapshots.')
@click.option('--client', default='default', show_default=True, type=str, help='The name of the client to use to connect to Google Cloud Storage.')
@click.option('--base_path', default='', show_default=True, type=str, help='Specifies the path within bucket to repository data. Defaults to the root of the bucket.')
@click.option('--chunk_size', type=str, help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--compress/--no-compress', default=True, show_default=True, help='Enable/Disable metadata compression.')
@click.option('--max_restore_rate', type=str, default='20mb', show_default=True, help='Throttles per node restore rate (per second).')
@click.option('--max_snapshot_rate', type=str, default='20mb', show_default=True, help='Throttles per node snapshot rate (per second).')
@click.option('--readonly', is_flag=True, help='If set, the repository is read-only.')
@click.option('--verify', is_flag=True, help='Verify repository after creation.')
@click.pass_context
def gcs(
        ctx, name, bucket, client, base_path, chunk_size, compress, max_restore_rate,
        max_snapshot_rate, readonly, verify
    ):
    """ Create a Google Cloud Storage repository.
    """
    gcs_settings = {
        'bucket': bucket,
        'client': client,
        'base_path': base_path,
        'chunk_size': chunk_size,
        'compress': compress,
        'max_restore_bytes_per_sec': max_restore_rate,
        'max_snapshot_bytes_per_sec': max_snapshot_rate,
        'readonly': readonly,
    }
    create_repo(ctx, repo_name=name, repo_type='gcs', repo_settings=gcs_settings, verify=verify)

# pylint: disable=unused-argument, invalid-name, line-too-long
@click.command(short_help='S3 Repository')
@click.option('--name', required=True, type=str, help='Repository name')
@click.option('--bucket', required=True, type=str, help='The bucket name must adhere to Amazon’s S3 bucket naming rules.')
@click.option('--client', default='default', show_default=True, type=str, help='The name of the S3 client to use to connect to S3.')
@click.option('--base_path', default='', show_default=True, type=str, help='Specifies the path to the repository data within its bucket. Defaults to an empty string, meaning that the repository is at the root of the bucket. The value of this setting should not start or end with a /.')
@click.option('--chunk_size', type=str, help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--compress/--no-compress', default=True, show_default=True, help='Enable/Disable metadata compression.')
@click.option('--max_restore_rate', type=str, default='20mb', show_default=True, help='Throttles per node restore rate (per second).')
@click.option('--max_snapshot_rate', type=str, default='20mb', show_default=True, help='Throttles per node snapshot rate (per second).')
@click.option('--readonly', is_flag=True, help='If set, the repository is read-only.')
@click.option('--server_side_encryption', is_flag=True, help='If set, files are encrypted on server side using AES256 algorithm.')
@click.option('--buffer_size', default='', type=str, help='Minimum threshold below which the chunk is uploaded using a single request. Must be between 5mb and 5gb.')
@click.option('--canned_acl', default='private', type=click.Choice(['private', 'public-read', 'public-read-write', 'authenticated-read', 'log-delivery-write', 'bucket-owner-read', 'bucket-owner-full-control']), help='When the S3 repository creates buckets and objects, it adds the canned ACL into the buckets and objects.')
@click.option('--storage_class', default='standard', type=click.Choice(['standard', 'reduced_redundancy', 'standard_ia', 'onezone_ia', 'intelligent_tiering']), help='Sets the S3 storage class for objects stored in the snapshot repository.')
@click.option('--verify', is_flag=True, help='Verify repository after creation.')
@click.pass_context
def s3(
        ctx, name, bucket, client, base_path, chunk_size, compress, max_restore_rate,
        max_snapshot_rate, readonly, server_side_encryption, buffer_size, canned_acl,
        storage_class, verify
    ):
    """
    Create an S3 repository.
    """
    s3_settings = {
        'bucket': bucket,
        'client': client,
        'base_path': base_path,
        'chunk_size': chunk_size,
        'compress': compress,
        'max_restore_bytes_per_sec': max_restore_rate,
        'max_snapshot_bytes_per_sec': max_snapshot_rate,
        'readonly': readonly,
        'server_side_encryption': server_side_encryption,
        'buffer_size': buffer_size,
        'canned_acl': canned_acl,
        'storage_class': storage_class
    }
    create_repo(ctx, repo_name=name, repo_type='s3', repo_settings=s3_settings, verify=verify)


# pylint: disable=unused-argument, invalid-name
@click.command(short_help='Shared Filesystem Repository')
@click.option('--name', required=True, type=str, help='Repository name')
@click.option('--location', required=True, type=str, help='Shared file-system location. Must match remote path, & be accessible to all master & data nodes')
@click.option('--compress/--no-compress', default=True, show_default=True, help='Enable/Disable metadata compression.')
@click.option('--chunk_size', type=str, help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--max_snapshots', default=2147483647, type=int, help='Maximum number of snapshots the repository can contain. Defaults to Integer.MAX_VALUE, which is 2147483647.')
@click.option('--max_restore_rate', type=str, default='20mb', show_default=True, help='Throttles per node restore rate (per second).')
@click.option('--max_snapshot_rate', type=str, default='20mb', show_default=True, help='Throttles per node snapshot rate (per second).')
@click.option('--readonly', is_flag=True, help='If set, the repository is read-only.')
@click.option('--verify', is_flag=True, help='Verify repository after creation.')
@click.pass_context
def fs(
        ctx, name, location, compress, chunk_size, max_snapshots,
        max_restore_rate, max_snapshot_rate, readonly, verify
    ):
    """
    Create a filesystem repository.
    """
    fs_settings = {
        'location': location,
        'compress': compress,
        'chunk_size': chunk_size,
        'max_number_of_snapshots': max_snapshots,
        'max_restore_bytes_per_sec': max_restore_rate,
        'max_snapshot_bytes_per_sec': max_snapshot_rate,
        'readonly': readonly,
    }
    create_repo(ctx, repo_name=name, repo_type='fs', repo_settings=fs_settings, verify=verify)


# pylint: disable=unused-argument, invalid-name
@click.command(short_help='Read-only URL Repository')
@click.option('--name', required=True, type=str, help='Repository name')
@click.option('--chunk_size', type=str, help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--http_max_retries', type=int, default=5, show_default=True, help='Maximum number of retries for http and https')
@click.option('--http_socket_timeout', type=str, default='50s', show_default=True, help='Maximum wait time for data transfers over a connection.')
@click.option('--compress/--no-compress', default=True, show_default=True, help='Enable/Disable metadata compression.')
@click.option('--max_snapshots', default=2147483647, type=int, help='Maximum number of snapshots the repository can contain. Defaults to Integer.MAX_VALUE, which is 2147483647.')
@click.option('--max_restore_rate', type=str, default='20mb', show_default=True, help='Throttles per node restore rate (per second).')
@click.option('--shared_filesystem_url', required=True, type=str, help='URL location of the root of the shared filesystem repository.')
@click.option('--verify', is_flag=True, help='Verify repository after creation.')
@click.pass_context
def url(
        ctx, name, chunk_size, http_max_retries, http_socket_timeout, compress,
        max_snapshots, max_restore_rate, shared_filesystem_url, verify
    ):
    """
    Create a read-only url repository.
    """
    url_settings = {
        'chunk_size': chunk_size,
        'http_max_retries': http_max_retries,
        'http_socket_timeout': http_socket_timeout,
        'compress': compress,
        'max_number_of_snapshots': max_snapshots,
        'max_restore_bytes_per_sec': max_restore_rate,
        'url': shared_filesystem_url
    }
    create_repo(ctx, repo_name=name, repo_type='url', repo_settings=url_settings, verify=verify)


# pylint: disable=unused-argument, invalid-name
@click.command(short_help='Source-Only Repository')
@click.option('--name', required=True, type=str, help='Repository name')
@click.option('--delegate_type', required=True, type=click.Choice(['azure', 'gcs', 's3', 'fs']), help='Delegated repository type.')
@click.option('--location', required=True, type=str, help='For Source-Only, specify the delegated repository name here')
@click.option('--compress/--no-compress', default=True, show_default=True, help='Enable/Disable metadata compression.')
@click.option('--chunk_size', type=str, help='Chunk size, e.g. 1g, 10m, 5k. [unbounded]')
@click.option('--max_snapshots', default=2147483647, type=int, help='Maximum number of snapshots the repository can contain. Defaults to Integer.MAX_VALUE, which is 2147483647.')
@click.option('--max_restore_rate', type=str, default='20mb', show_default=True, help='Throttles per node restore rate (per second).')
@click.option('--max_snapshot_rate', type=str, default='20mb', show_default=True, help='Throttles per node snapshot rate (per second).')
@click.option('--readonly', is_flag=True, help='If set, the repository is read-only.')
@click.option('--verify', is_flag=True, help='Verify repository after creation.')
@click.pass_context
def source(
        ctx, name, delegate_type, location, compress, chunk_size, max_snapshots,
        max_restore_rate, max_snapshot_rate, readonly, verify
    ):
    """
    Create a filesystem repository.
    """
    source_settings = {
        'chunk_size': chunk_size,
        'compress': compress,
        'delegate_type': delegate_type,
        'location': location,
        'max_number_of_snapshots': max_snapshots,
        'max_restore_bytes_per_sec': max_restore_rate,
        'max_snapshot_bytes_per_sec': max_snapshot_rate,
        'readonly': readonly,
    }
    create_repo(ctx, repo_name=name, repo_type='source', repo_settings=source_settings, verify=verify)


# pylint: disable=unused-argument, redefined-builtin
@click.group(context_settings=get_width())
@click.option('--config', help='Path to configuration file.', type=click.Path(exists=True), default=settings.config_file())
@click.option('--hosts', help='Elasticsearch URL to connect to', multiple=True)
@click.option('--cloud_id', help='Shorthand to connect to Elastic Cloud instance')
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
@click.option('--dry-run', is_flag=True, help='Do not perform any changes. NON-FUNCTIONAL PLACEHOLDER! DO NOT USE!')
@click.option('--loglevel', help='Log level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
@click.option('--logfile', help='log file')
@click.option('--logformat', help='Log output format', type=click.Choice(['default', 'logstash', 'json', 'ecs']))
@click.version_option(version=__version__)
@click.pass_context
def repo_mgr_cli(
    ctx, config, hosts, cloud_id, id, api_key, username, password, bearer_auth,
    opaque_id, request_timeout, http_compress, verify_certs, ca_certs, client_cert, client_key,
    ssl_assert_hostname, ssl_assert_fingerprint, ssl_version, master_only, skip_version_test,
    dry_run, loglevel, logfile, logformat
):
    """Repository manager for Elasticsearch Curator."""
    # Ensure a passable ctx object
    ctx.ensure_object(dict)

    # Extract client args
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
    logger = logging.getLogger(__name__)
    logger.debug('Logging options validated.')

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
            'api_key': api_key
        }
    })

    # Remove `api_key` root key if `id` and `api_key` are both None
    if id is None and api_key is None:
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
    ctx.obj['esconfig'] = final_config
    ctx.obj['dry_run'] = dry_run
    logger.debug('YOU HAVE REACHED THIS PHASE')

@repo_mgr_cli.group('create')
@click.pass_context
def _create(ctx):
    """Create an Elasticsearch repository"""
_create.add_command(azure)
_create.add_command(gcs)
_create.add_command(s3)
_create.add_command(fs)
_create.add_command(url)
_create.add_command(source)

@repo_mgr_cli.command('show')
@click.pass_context
def show(ctx):
    """Show all repositories"""
    client = get_client(ctx)
    show_repos(client)

@repo_mgr_cli.command('delete')
@click.option('--name', required=True, help='Repository name', type=str)
@click.option('--yes', is_flag=True, callback=delete_callback, expose_value=False, prompt='Are you sure you want to delete the repository?')
@click.pass_context
def _delete(ctx, name):
    """
    Delete an Elasticsearch repository
    """
    logger = logging.getLogger('curator.repomgrcli._delete')
    client = get_client(ctx)
    try:
        logger.info('Deleting repository %s...', name)
        client.snapshot.delete_repository(name=name)
    except NotFoundError:
        logger.error('Unable to delete repository: %s  Not Found.', name)
        sys.exit(1)

@repo_mgr_cli.command('info')
@click.pass_context
def info(ctx):
    """Show cluster info"""
    client = get_client(ctx)
    pp = pprint.PrettyPrinter(indent=4)
    output = dict(client.info())
    click.echo(f'{pp.pprint(output)}')
