import os, sys
import yaml
import logging
import click
from voluptuous import Schema
from curator import actions
from curator.config_utils import process_config, password_filter
from curator.defaults import settings
from curator.exceptions import NoIndices, NoSnapshots
from curator.indexlist import IndexList
from curator.snapshotlist import SnapshotList
from curator.utils import get_client, get_yaml, prune_nones, validate_actions
from curator.validators import SchemaCheck
from curator._version import __version__

CLASS_MAP = {
    'alias' : actions.Alias,
    'allocation' : actions.Allocation,
    'close' : actions.Close,
    'cluster_routing' : actions.ClusterRouting,
    'create_index' : actions.CreateIndex,
    'delete_indices' : actions.DeleteIndices,
    'delete_snapshots' : actions.DeleteSnapshots,
    'forcemerge' : actions.ForceMerge,
    'index_settings' : actions.IndexSettings,
    'open' : actions.Open,
    'reindex' : actions.Reindex,
    'replicas' : actions.Replicas,
    'restore' : actions.Restore,
    'rollover' : actions.Rollover,
    'snapshot' : actions.Snapshot,
    'shrink' : actions.Shrink,
}

def process_action(client, config, **kwargs):
    """
    Do the `action` in the configuration dictionary, using the associated args.
    Other necessary args may be passed as keyword arguments

    :arg config: An `action` dictionary.
    """
    logger = logging.getLogger(__name__)
    # Make some placeholder variables here for readability
    logger.debug('Configuration dictionary: {0}'.format(config))
    logger.debug('kwargs: {0}'.format(kwargs))
    action = config['action']
    # This will always have some defaults now, so no need to do the if...
    # # OLD WAY: opts = config['options'] if 'options' in config else {}
    opts = config['options']
    logger.debug('opts: {0}'.format(opts))
    mykwargs = {}

    action_class = CLASS_MAP[action]

    # Add some settings to mykwargs...
    if action == 'delete_indices':
        mykwargs['master_timeout'] = (
            kwargs['master_timeout'] if 'master_timeout' in kwargs else 30)

    ### Update the defaults with whatever came with opts, minus any Nones
    mykwargs.update(prune_nones(opts))
    logger.debug('Action kwargs: {0}'.format(mykwargs))

    ### Set up the action ###
    if action == 'alias':
        # Special behavior for this action, as it has 2 index lists
        logger.debug('Running "{0}" action'.format(action.upper()))
        action_obj = action_class(**mykwargs)
        removes = IndexList(client)
        adds = IndexList(client)
        if 'remove' in config:
            logger.debug(
                'Removing indices from alias "{0}"'.format(opts['name']))
            removes.iterate_filters(config['remove'])
            action_obj.remove(
                removes, warn_if_no_indices= opts['warn_if_no_indices'])
        if 'add' in config:
            logger.debug('Adding indices to alias "{0}"'.format(opts['name']))
            adds.iterate_filters(config['add'])
            action_obj.add(adds, warn_if_no_indices=opts['warn_if_no_indices'])
    elif action in [ 'cluster_routing', 'create_index', 'rollover']:
        action_obj = action_class(client, **mykwargs)
    elif action == 'delete_snapshots' or action == 'restore':
        logger.debug('Running "{0}"'.format(action))
        slo = SnapshotList(client, repository=opts['repository'])
        slo.iterate_filters(config)
        # We don't need to send this value to the action
        mykwargs.pop('repository')
        action_obj = action_class(slo, **mykwargs)
    else:
        logger.debug('Running "{0}"'.format(action.upper()))
        ilo = IndexList(client)
        ilo.iterate_filters(config)
        action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    if 'dry_run' in kwargs and kwargs['dry_run'] == True:
        action_obj.do_dry_run()
    else:
        logger.debug('Doing the action here.')
        action_obj.do_action()

def run(config, action_file, dry_run=False):
    """
    Actually run.
    """
    client_args = process_config(config)
    logger = logging.getLogger(__name__)
    logger.debug('Client and logging options validated.')

    # Extract this and save it for later, in case there's no timeout_override.
    default_timeout = client_args.pop('timeout')
    logger.debug('default_timeout = {0}'.format(default_timeout))
    #########################################
    ### Start working on the actions here ###
    #########################################
    logger.debug('action_file: {0}'.format(action_file))
    action_config = get_yaml(action_file)
    logger.debug('action_config: {0}'.format(password_filter(action_config)))
    action_dict = validate_actions(action_config)
    actions = action_dict['actions']
    logger.debug('Full list of actions: {0}'.format(password_filter(actions)))
    action_keys = sorted(list(actions.keys()))
    for idx in action_keys:
        action = actions[idx]['action']
        action_disabled = actions[idx]['options'].pop('disable_action')
        logger.debug('action_disabled = {0}'.format(action_disabled))
        continue_if_exception = (
            actions[idx]['options'].pop('continue_if_exception'))
        logger.debug(
            'continue_if_exception = {0}'.format(continue_if_exception))
        timeout_override = actions[idx]['options'].pop('timeout_override')
        logger.debug('timeout_override = {0}'.format(timeout_override))
        ignore_empty_list = actions[idx]['options'].pop('ignore_empty_list')
        logger.debug('ignore_empty_list = {0}'.format(ignore_empty_list))
        allow_ilm = actions[idx]['options'].pop('allow_ilm_indices')
        logger.debug('allow_ilm_indices = {0}'.format(allow_ilm))
        ### Filter ILM indices unless expressly permitted
        if not allow_ilm and action not in settings.snapshot_actions():
            if 'filters' in actions[idx]:
                actions[idx]['filters'].append({'filtertype': 'ilm'})
            else:
                actions[idx]['filters'] = [{'filtertype': 'ilm'}]
        ### Skip to next action if 'disabled'
        if action_disabled:
            logger.info(
                'Action ID: {0}: "{1}" not performed because "disable_action" '
                'is set to True'.format(idx, action)
            )
            continue
        else:
            logger.info('Preparing Action ID: {0}, "{1}"'.format(idx, action))
        # Override the timeout, if specified, otherwise use the default.
        if isinstance(timeout_override, int):
            client_args['timeout'] = timeout_override
        else:
            client_args['timeout'] = default_timeout

        # Set up action kwargs
        kwargs = {}
        kwargs['master_timeout'] = (
            client_args['timeout'] if client_args['timeout'] <= 300 else 300)
        kwargs['dry_run'] = dry_run

        # Create a client object for each action...
        client = get_client(**client_args)
        logger.debug('client is {0}'.format(type(client)))
        ##########################
        ### Process the action ###
        ##########################
        try:
            logger.info('Trying Action ID: {0}, "{1}": '
                '{2}'.format(idx, action, actions[idx]['description'])
            )
            process_action(client, actions[idx], **kwargs)
        except Exception as e:
            if isinstance(e, NoIndices) or isinstance(e, NoSnapshots):
                if ignore_empty_list:
                    logger.info(
                        'Skipping action "{0}" due to empty list: '
                        '{1}'.format(action, type(e))
                    )
                else:
                    logger.error(
                        'Unable to complete action "{0}".  No actionable items '
                        'in list: {1}'.format(action, type(e))
                    )
                    sys.exit(1)
            else:
                logger.error(
                    'Failed to complete action: {0}.  {1}: '
                    '{2}'.format(action, type(e), e)
                )
                if continue_if_exception:
                    logger.info(
                        'Continuing execution with next action because '
                        '"continue_if_exception" is set to True for action '
                        '{0}'.format(action)
                    )
                else:
                    sys.exit(1)
        logger.info('Action ID: {0}, "{1}" completed.'.format(idx, action))
    logger.info('Job completed.')

@click.command()
@click.option('--config',
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
