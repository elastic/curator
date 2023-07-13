"""Main CLI for Curator"""
import sys
import logging
import pathlib
import click
from es_client.builder import ClientArgs, OtherArgs
from es_client.helpers.utils import get_yaml, check_config, prune_nones, verify_url_schema
from curator.exceptions import ClientException
from curator.classdef import ActionsFile
from curator.config_utils import check_logging_config, set_logging
from curator.defaults import settings
from curator.exceptions import NoIndices, NoSnapshots
from curator.helpers.getters import get_client
from curator.helpers.testers import ilm_policy_check
from curator.cli_singletons.utils import get_width
from curator._version import __version__

def configfile_callback(ctx, param, value):
    """Callback to validate whether the provided config file exists and is writeable

    :param ctx: The click context
    :param param: The click parameter object
    :param value: The value of the parameter

    :type ctx: Click context
    :type param: Click object
    :type value: Any

    :returns: Config file path or None
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    logger.debug('Click ctx = %s', ctx)
    logger.debug('Click param = %s', param)
    logger.debug('Click value = %s', value)
    path = pathlib.Path(value)
    if path.is_file():
        return value
    logger.warning('Config file not found: %s', value)
    return None

def override_logging(config, loglevel, logfile, logformat):
    """Get logging config and override from command-line options

    :param config: The configuration from file
    :param loglevel: The log level
    :param logfile: The log file to write
    :param logformat: Which log format to use

    :type config: dict
    :type loglevel: str
    :type logfile: str
    :type logformat: str

    :returns: Log configuration ready for validation
    :rtype: dict
    """
    # Check for log settings from config file
    init_logcfg = check_logging_config(config)

    # Override anything with options from the command-line
    if loglevel:
        init_logcfg['loglevel'] = loglevel
    if logfile:
        init_logcfg['logfile'] = logfile
    if logformat:
        init_logcfg['logformat'] = logformat
    return init_logcfg

def cli_hostslist(hosts):
    """
    :param hosts: One or more hosts.
    :type hosts: str or list

    :returns: A list of hosts that came in from the command-line, or ``None``
    :rtype: list or ``None``
    """
    hostslist = []
    if hosts:
        for host in list(hosts):
            hostslist.append(verify_url_schema(host))
    else:
        hostslist = None
    return hostslist

def ilm_action_skip(client, action_def):
    """
    Skip rollover action if ``allow_ilm_indices`` is ``false``. For all other non-snapshot actions,
    add the ``ilm`` filtertype to the :py:attr:`~.curator.ActionDef.filters` list.

    :param action_def: An action object
    :type action_def: :py:class:`~.curator.classdef.ActionDef`

    :returns: ``True`` if ``action_def.action`` is ``rollover`` and the alias identified by
        ``action_def.options['name']`` is associated with an ILM policy. This hacky work-around is
        because the Rollover action does not use :py:class:`~.curator.IndexList`
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    if not action_def.allow_ilm and action_def.action not in settings.snapshot_actions():
        if action_def.action == 'rollover':
            if ilm_policy_check(client, action_def.options['name']):
                logger.info('Alias %s is associated with ILM policy.', action_def.options['name'])
                # logger.info('Skipping action %s because allow_ilm_indices is false.', idx)
                return True
        elif action_def.filters:
            action_def.filters.append({'filtertype': 'ilm'})
        else:
            action_def.filters = [{'filtertype': 'ilm'}]
    return False

def exception_handler(action_def, err):
    """Do the grunt work with the exception

    :param action_def: An action object
    :param err: The exception

    :type action_def: :py:class:`~.curator.classdef.ActionDef`
    :type err: :py:exc:`Exception`
    """
    logger = logging.getLogger(__name__)
    if isinstance(err, (NoIndices, NoSnapshots)):
        if action_def.iel:
            logger.info(
                'Skipping action "%s" due to empty list: %s', action_def.action, type(err))
        else:
            logger.error(
                'Unable to complete action "%s".  No actionable items in list: %s',
                action_def.action, type(err))
            sys.exit(1)
    else:
        logger.error(
            'Failed to complete action: %s.  %s: %s', action_def.action, type(err), err)
        if action_def.cif:
            logger.info(
                'Continuing execution with next action because "continue_if_exception" '
                'is set to True for action %s', action_def.action)
        else:
            sys.exit(1)

def process_action(client, action_def, dry_run=False):
    """
    Do the ``action`` in ``action_def.action``, using the associated options and any ``kwargs``.

    :param client: A client connection object
    :param action_def: The ``action`` object

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type action_def: :py:class:`~.curator.classdef.ActionDef`
    :rtype: None
    """
    logger = logging.getLogger(__name__)
    logger.debug('Configuration dictionary: %s', action_def.action_dict)
    mykwargs = {}

    # Add some settings to mykwargs...
    if action_def.action == 'delete_indices':
        mykwargs['master_timeout'] = 30

    ### Update the defaults with whatever came with opts, minus any Nones
    mykwargs.update(prune_nones(action_def.options))
    logger.debug('Action kwargs: %s', mykwargs)

    ### Set up the action ###
    logger.debug('Running "%s"', action_def.action.upper())
    if action_def.action == 'alias':
        # Special behavior for this action, as it has 2 index lists
        action_def.instantiate('action_cls', **mykwargs)
        action_def.instantiate('alias_adds', client)
        action_def.instantiate('alias_removes', client)
        if 'remove' in action_def.action_dict:
            logger.debug('Removing indices from alias "%s"', action_def.options['name'])
            action_def.alias_removes.iterate_filters(action_def.action_dict['remove'])
            action_def.action_cls.remove(
                action_def.alias_removes,
                warn_if_no_indices=action_def.options['warn_if_no_indices'])
        if 'add' in action_def.action_dict:
            logger.debug('Adding indices to alias "%s"', action_def.options['name'])
            action_def.alias_adds.iterate_filters(action_def.action_dict['add'])
            action_def.action_cls.add(
                action_def.alias_adds, warn_if_no_indices=action_def.options['warn_if_no_indices'])
    elif action_def.action in ['cluster_routing', 'create_index', 'rollover']:
        action_def.instantiate('action_cls', client, **mykwargs)
    else:
        if action_def.action in ['delete_snapshots', 'restore']:
            mykwargs.pop('repository') # We don't need to send this value to the action
            action_def.instantiate('list_obj', client, repository=action_def.options['repository'])
        else:
            action_def.instantiate('list_obj', client)
        action_def.list_obj.iterate_filters({'filters': action_def.filters})
        action_def.instantiate('action_cls', action_def.list_obj, **mykwargs)
    ### Do the action
    if dry_run:
        action_def.action_cls.do_dry_run()
    else:
        logger.debug('Doing the action here.')
        action_def.action_cls.do_action()

def run(client_args, other_args, action_file, dry_run=False):
    """
    Called by :py:func:`cli` to execute what was collected at the command-line

    :param client_args: The ClientArgs arguments object
    :param other_args: The OtherArgs arguments object
    :param action_file: The action configuration file
    :param dry_run: Do not perform any changes

    :type client_args: :py:class:`~.es_client.ClientArgs`
    :type other_args: :py:class:`~.es_client.OtherArgs`
    :type action_file: str
    :type dry_run: bool
    """
    logger = logging.getLogger(__name__)

    logger.debug('action_file: %s', action_file)
    all_actions = ActionsFile(action_file)
    for idx in sorted(list(all_actions.actions.keys())):
        action_def = all_actions.actions[idx]
        ### Skip to next action if 'disabled'
        if action_def.disabled:
            logger.info(
                'Action ID: %s: "%s" not performed because "disable_action" '
                'is set to True', idx, action_def.action
            )
            continue
        logger.info('Preparing Action ID: %s, "%s"', idx, action_def.action)

        # Override the timeout, if specified, otherwise use the default.
        if action_def.timeout_override:
            client_args.request_timeout = action_def.timeout_override

        # Create a client object for each action...
        logger.info('Creating client object and testing connection')

        try:
            client = get_client(configdict={
                'elasticsearch': {
                    'client': prune_nones(client_args.asdict()),
                    'other_settings': prune_nones(other_args.asdict())
                }
            })
        except ClientException as exc:
            # No matter where logging is set to go, make sure we dump these messages to the CLI
            click.echo('Unable to establish client connection to Elasticsearch!')
            click.echo(f'Exception: {exc}')
            sys.exit(1)

        ### Filter ILM indices unless expressly permitted
        if ilm_action_skip(client, action_def):
            continue
        ##########################
        ### Process the action ###
        ##########################
        msg = f'Trying Action ID: {idx}, "{action_def.action}": {action_def.description}'
        try:
            logger.info(msg)
            process_action(client, action_def, dry_run=dry_run)
        # pylint: disable=broad-except
        except Exception as err:
            exception_handler(action_def, err)
        logger.info('Action ID: %s, "%s" completed.', idx, action_def.action)
    logger.info('All actions completed.')

# pylint: disable=unused-argument, redefined-builtin
@click.command(context_settings=get_width())
@click.option('--config', help='Path to configuration file.', type=str, default=settings.config_file(), callback=configfile_callback)
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
@click.argument('action_file', type=click.Path(exists=True), nargs=1)
@click.version_option(version=__version__)
@click.pass_context
def cli(
    ctx, config, hosts, cloud_id, api_token, id, api_key, username, password, bearer_auth,
    opaque_id, request_timeout, http_compress, verify_certs, ca_certs, client_cert, client_key,
    ssl_assert_hostname, ssl_assert_fingerprint, ssl_version, master_only, skip_version_test,
    dry_run, loglevel, logfile, logformat, action_file
):
    """
    Curator for Elasticsearch indices.

    See http://elastic.co/guide/en/elasticsearch/client/curator/current
    """
    client_args = ClientArgs()
    other_args = OtherArgs()
    if config:
        from_yaml = get_yaml(config)
    else:
        # Use empty defaults.
        from_yaml = {'elasticsearch': {'client': {}, 'other_settings': {}}, 'logging': {}}
    raw_config = check_config(from_yaml)
    client_args.update_settings(raw_config['client'])
    other_args.update_settings(raw_config['other_settings'])

    set_logging(check_logging_config(
        {'logging': override_logging(from_yaml, loglevel, logfile, logformat)}))

    hostslist = cli_hostslist(hosts)

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
    run(client_args, other_args, action_file, dry_run)
