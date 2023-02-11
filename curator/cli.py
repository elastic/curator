"""Main CLI for Curator"""
import sys
import logging
import click
from es_client.builder import ClientArgs, OtherArgs, Builder
from es_client.helpers.utils import get_yaml, check_config, prune_nones
from curator.actions import (
    Alias, Allocation, Close, ClusterRouting, CreateIndex, DeleteIndices, ForceMerge,
    IndexSettings, Open, Reindex, Replicas, Rollover, Shrink, Snapshot, DeleteSnapshots, Restore
)
from curator.exceptions import ConfigurationError, ClientException
from curator.config_utils import check_logging_config, password_filter, set_logging
from curator.defaults import settings
from curator.exceptions import NoIndices, NoSnapshots
from curator.helpers.getters import get_write_index
from curator.helpers.testers import validate_actions
from curator.indexlist import IndexList
from curator.snapshotlist import SnapshotList
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

def run(config, action_file, dry_run=False):
    """
    Called by :py:func:`cli` to execute what was collected at the command-line
    """
    # """Process yaml_file and return a valid client configuration"""
    config_dict = get_yaml(config)
    if config_dict is None:
        click.echo('Empty configuration file provided. Using defaults')
        config_dict = {}
    elif not isinstance(config_dict, dict):
        raise ConfigurationError('Configuration file not converted to dictionary. Check YAML configuration.')
    set_logging(check_logging_config(config_dict))
    # set_logging({'loglevel':'DEBUG','blacklist':[]})
    logger = logging.getLogger(__name__)
    if not isinstance(config_dict, dict):
        config_dict = {}
        logger.warning(
            'Provided config file %s was unable to be properly read, or is empty. '
            'Using empty dictionary (assuming defaults)', config)
    logger.debug('config_dict = %s', config_dict)
    client_args = ClientArgs()
    other_args = OtherArgs()
    if config:
        raw_config = check_config(config_dict)
        logger.debug('raw_config = %s', raw_config)
        try:
            client_args.update_settings(raw_config['client'])
        # pylint: disable=broad-except
        except Exception as exc:
            click.echo(f'EXCEPTION = {exc}')
            sys.exit(1)
        other_args.update_settings(raw_config['other_settings'])

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

        # Build a "final_config" that reflects CLI args overriding anything from a config_file
        final_config = {
            'elasticsearch': {
                'client': prune_nones(client_args.asdict()),
                'other_settings': prune_nones(other_args.asdict())
            }
        }
        builder = Builder(configdict=final_config)

        try:
            builder.connect()
        except Exception as exc:
            click.echo(f'Exception encountered: {exc}')
            raise ClientException from exc

        client = builder.client
        ### Filter ILM indices unless expressly permitted
        if allow_ilm:
            logger.warning('allow_ilm_indices: true')
            logger.warning('Permitting operation on indices with an ILM policy')
        if not allow_ilm and action not in settings.snapshot_actions():
            if actions_config[idx]['action'] == 'rollover':
                alias = actions_config[idx]['options']['name']
                write_index = get_write_index(client, alias)
                try:
                    idx_settings = client.indices.get_settings(index=write_index)
                    if 'name' in idx_settings[write_index]['settings']['index']['lifecycle']:
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
            if isinstance(err, NoIndices) or isinstance(err, NoSnapshots):
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

@click.command()
@click.option(
    '--config',
    help="Path to configuration file. Default: ~/.curator/curator.yml",
    type=click.Path(exists=True), default=settings.config_file()
)
@click.option('--dry-run', is_flag=True, help='Do not perform any changes.')
@click.argument('action_file', type=click.Path(exists=True), nargs=1)
@click.version_option(version=__version__)
def cli(config, dry_run, action_file):
    """
    Curator for Elasticsearch indices.

    See http://elastic.co/guide/en/elasticsearch/client/curator/current
    """
    run(config, action_file, dry_run)
