"""Main CLI for Curator"""
import sys
import logging
import click
from es_client.builder import ClientArgs, OtherArgs
from es_client.helpers.utils import get_yaml, check_config, prune_nones, verify_url_schema
from curator.actions import (
    Alias, Allocation, Close, ClusterRouting, CreateIndex, DeleteIndices, ForceMerge,
    IndexSettings, Open, Reindex, Replicas, Rollover, Shrink, Snapshot, DeleteSnapshots, Restore
)
from curator.exceptions import ConfigurationError, ClientException
from curator.config_utils import check_logging_config, password_filter, set_logging
from curator.defaults import settings
from curator.exceptions import NoIndices, NoSnapshots
from curator.helpers.getters import get_client
from curator.helpers.testers import ilm_policy_check, validate_actions
from curator.indexlist import IndexList
from curator.snapshotlist import SnapshotList
from curator.cli_singletons.utils import get_width
from curator._version import __version__

CLASS_MAP = {
    'alias' : Alias,
    'allocation' : Allocation,
    'close' : Close,
    'cluster_routing' : ClusterRouting,
    'create_index' : CreateIndex,
    'delete_indices' : DeleteIndices,
    'delete_snapshots' : DeleteSnapshots,
    'forcemerge' : ForceMerge,
    'index_settings' : IndexSettings,
    'open' : Open,
    'reindex' : Reindex,
    'replicas' : Replicas,
    'restore' : Restore,
    'rollover' : Rollover,
    'snapshot' : Snapshot,
    'shrink' : Shrink,
}

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

def process_action(client, config, **kwargs):
    """
    Do the ``action`` in ``config``, using the associated options and ``kwargs``, if any.

    :param config: ``action`` configuration data.

    :type config: dict
    :rtype: None
    """
    logger = logging.getLogger(__name__)
    # Make some placeholder variables here for readability
    logger.debug('Configuration dictionary: %s', config)
    logger.debug('kwargs: %s', kwargs)
    action = config['action']
    # This will always have some defaults now, so no need to do the if...
    # # OLD WAY: opts = config['options'] if 'options' in config else {}
    opts = config['options']
    logger.debug('opts: %s', opts)
    mykwargs = {}

    action_class = CLASS_MAP[action]

    # Add some settings to mykwargs...
    if action == 'delete_indices':
        mykwargs['master_timeout'] = (
            kwargs['master_timeout'] if 'master_timeout' in kwargs else 30)

    ### Update the defaults with whatever came with opts, minus any Nones
    mykwargs.update(prune_nones(opts))
    logger.debug('Action kwargs: %s', mykwargs)

    ### Set up the action ###
    if action == 'alias':
        # Special behavior for this action, as it has 2 index lists
        logger.debug('Running "%s" action', action.upper())
        action_obj = action_class(**mykwargs)
        removes = IndexList(client)
        adds = IndexList(client)
        if 'remove' in config:
            logger.debug(
                'Removing indices from alias "%s"', opts['name'])
            removes.iterate_filters(config['remove'])
            action_obj.remove(
                removes, warn_if_no_indices=opts['warn_if_no_indices'])
        if 'add' in config:
            logger.debug('Adding indices to alias "%s"', opts['name'])
            adds.iterate_filters(config['add'])
            action_obj.add(adds, warn_if_no_indices=opts['warn_if_no_indices'])
    elif action in ['cluster_routing', 'create_index', 'rollover']:
        action_obj = action_class(client, **mykwargs)
    elif action in ['delete_snapshots', 'restore']:
        logger.debug('Running "%s"', action)
        slo = SnapshotList(client, repository=opts['repository'])
        slo.iterate_filters(config)
        # We don't need to send this value to the action
        mykwargs.pop('repository')
        action_obj = action_class(slo, **mykwargs)
    else:
        logger.debug('Running "%s"', action.upper())
        ilo = IndexList(client)
        ilo.iterate_filters(config)
        action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    if 'dry_run' in kwargs and kwargs['dry_run']:
        action_obj.do_dry_run()
    else:
        logger.debug('Doing the action here.')
        action_obj.do_action()

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

    logger.debug('Client and logging configuration options validated.')

    #########################################
    ### Start working on the actions here ###
    #########################################
    logger.debug('action_file: %s', action_file)
    action_config = get_yaml(action_file)
    logger.debug('action_config: %s', password_filter(action_config))
    try:
        action_dict = validate_actions(action_config)
    except (ConfigurationError, UnboundLocalError) as err:
        logger.critical('Configuration Error: %s', err)
        click.echo(f'Critical configuration error encountered: {err}')
        raise ConfigurationError from err
    actions_config = action_dict['actions']
    logger.debug('Full list of actions: %s', password_filter(actions_config))
    action_keys = sorted(list(actions_config.keys()))
    for idx in action_keys:
        action = actions_config[idx]['action']
        action_disabled = actions_config[idx]['options'].pop('disable_action')
        logger.debug('action_disabled = %s', action_disabled)
        continue_if_exception = (actions_config[idx]['options'].pop('continue_if_exception'))
        logger.debug('continue_if_exception = %s', continue_if_exception)
        timeout_override = actions_config[idx]['options'].pop('timeout_override')
        logger.debug('timeout_override = %s', timeout_override)
        ignore_empty_list = actions_config[idx]['options'].pop('ignore_empty_list')
        logger.debug('ignore_empty_list = %s', ignore_empty_list)
        allow_ilm = actions_config[idx]['options'].pop('allow_ilm_indices')
        logger.debug('allow_ilm_indices = %s', allow_ilm)

        ### Skip to next action if 'disabled'
        if action_disabled:
            logger.info(
                'Action ID: %s: "%s" not performed because "disable_action" '
                'is set to True', idx, action
            )
            continue

        logger.info('Preparing Action ID: %s, "%s"', idx, action)
        # Override the timeout, if specified, otherwise use the default.
        if isinstance(timeout_override, int):
            client_args.request_timeout = timeout_override

        # Set up action kwargs
        kwargs = {}
        kwargs['dry_run'] = dry_run

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
        if allow_ilm:
            logger.warning('allow_ilm_indices: true')
            logger.warning('Permitting operation on indices with an ILM policy')
        if not allow_ilm and action not in settings.snapshot_actions():
            if actions_config[idx]['action'] == 'rollover':
                alias = actions_config[idx]['options']['name']
                try:
                    if ilm_policy_check(client, alias):
                        logger.info('Alias %s is associated with ILM policy.', alias)
                        logger.info('Skipping action %s because allow_ilm_indices is false.', idx)
                        continue
                except KeyError:
                    logger.debug('No ILM policies associated with %s', alias)
            elif 'filters' in actions_config[idx]:
                actions_config[idx]['filters'].append({'filtertype': 'ilm'})
            else:
                actions_config[idx]['filters'] = [{'filtertype': 'ilm'}]
        ##########################
        ### Process the action ###
        ##########################
        try:
            logger.info(
                'Trying Action ID: %s, "%s": %s', idx, action, actions_config[idx]['description']
            )
            process_action(client, actions_config[idx], **kwargs)
        # pylint: disable=broad-except
        except Exception as err:
            if isinstance(err, (NoIndices, NoSnapshots)):
                if ignore_empty_list:
                    logger.info('Skipping action "%s" due to empty list: %s', action, type(err))
                else:
                    logger.error('Unable to complete action "%s".  No actionable items in list: %s', action, type(err))
                    sys.exit(1)
            else:
                logger.error('Failed to complete action: %s.  %s: %s', action, type(err), err)
                if continue_if_exception:
                    logger.info('Continuing execution with next action because "continue_if_exception" is set to True for action %s', action)
                else:
                    sys.exit(1)
        logger.info('Action ID: %s, "%s" completed.', idx, action)
    logger.info('Job completed.')

@click.command(context_settings=get_width())
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
@click.option('--dry-run', is_flag=True, help='Do not perform any changes.')
@click.option('--loglevel', help='Log level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
@click.option('--logfile', help='log file')
@click.option('--logformat', help='Log output format', type=click.Choice(['default', 'logstash', 'json', 'ecs']))
@click.argument('action_file', type=click.Path(exists=True), nargs=1)
@click.version_option(version=__version__)
@click.pass_context
def cli(
    ctx, config, hosts, cloud_id, id, api_key, username, password, bearer_auth,
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
    run(client_args, other_args, action_file, dry_run)