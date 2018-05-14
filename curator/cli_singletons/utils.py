import click
import json
import logging
import sys
from curator.actions import Alias, Allocation, Close, ClusterRouting, CreateIndex, DeleteIndices, DeleteSnapshots, ForceMerge, Open, Replicas, Restore, Shrink, Snapshot
from curator.exceptions import NoIndices, NoSnapshots
from curator.validators import SchemaCheck, config_file, options
from curator.utils import ensure_list, filters, prune_nones, validate_filters
from voluptuous import Schema



CLASS_MAP = {
    'alias' :  Alias,
    'allocation' : Allocation,
    'close' : Close,
    'cluster_routing' : ClusterRouting,
    'create_index' : CreateIndex,
    'delete_indices' : DeleteIndices,
    'delete_snapshots' : DeleteSnapshots,
    'forcemerge' : ForceMerge,
    'open' : Open,
    'replicas' : Replicas,
    'restore' : Restore,
    'snapshot' : Snapshot,
}

EXCLUDED_OPTIONS = [
    'ignore_empty_list', 'timeout_override',
    'continue_if_exception', 'disable_action'
]

def get_width():
    return dict(max_content_width=click.get_terminal_size()[0])

def json_to_dict(ctx, param, value):
    if value is None:
        return {}
    try:
        return json.loads(value)
    except ValueError:
        raise click.BadParameter('Invalid JSON for {0}: {1}'.format(param, value))

def validate_filter_json(ctx, param, value):
    # The "None" edge case should only be for optional filters, like alias add/remove
    if value is None:
        return value
    try:
        filter_list = ensure_list(json.loads(value))
        return filter_list
    except ValueError:
        raise click.BadParameter('Filter list is invalid JSON: {0}'.format(value))

def false_to_none(ctx, param, value):
    try:
        if value:
            return True
        else:
            return None
    except ValueError:
        raise click.BadParameter('Invalid value: {0}'.format(value))

def filter_schema_check(action, filter_dict):
    valid_filters = SchemaCheck(
        filter_dict,
        Schema(filters.Filters(action, location='singleton')),
        'filters',
        '{0} singleton action "filters"'.format(action)
    ).result()
    return validate_filters(action, valid_filters)

def actionator(action, action_obj, dry_run=True):
    logger = logging.getLogger(__name__)
    logger.debug('Doing the singleton "{0}" action here.'.format(action))
    try:
        if dry_run:
            action_obj.do_dry_run()
        else:
            action_obj.do_action()
    except Exception as e:
        if isinstance(e, NoIndices) or isinstance(e, NoSnapshots):
            logger.error(
                'Unable to complete action "{0}".  No actionable items '
                'in list: {1}'.format(action, type(e))
            )
        else:
            logger.error(
                'Failed to complete action: {0}.  {1}: '
                '{2}'.format(action, type(e), e)
            )
        sys.exit(1)
    logger.info('Singleton "{0}" action completed.'.format(action))

def do_filters(list_object, filters, ignore=False):
    logger = logging.getLogger(__name__)
    logger.debug('Running filters and testing for empty list object')
    try:
        list_object.iterate_filters(filters)
        list_object.empty_list_check()
    except (NoIndices, NoSnapshots) as e:
        if isinstance(e, NoIndices):
            otype = 'index'
        else:
            otype = 'snapshot'
        if ignore:
            logger.info(
                'Singleton action not performed: empty {0} list'.format(otype)
            )
            sys.exit(0)
        else:
            logger.error(
                'Singleton action failed due to empty {0} list'.format(otype)
            )
            sys.exit(1)


def prune_excluded(option_dict):
    for k in list(option_dict.keys()):
        if k in EXCLUDED_OPTIONS:
            del option_dict[k]
    return option_dict

def option_schema_check(action, option_dict):
    clean_options = SchemaCheck(
        prune_nones(option_dict),
        options.get_schema(action),
        'options',
        '{0} singleton action "options"'.format(action)
    ).result()
    return prune_excluded(clean_options)

def config_override(ctx, config_dict):
    if config_dict == None:
        config_dict = {}
    for k in ['client', 'logging']:
        if not k in config_dict:
            config_dict[k] = {}
    for k in list(ctx.params.keys()):
        if k in ['dry_run', 'config']:
            pass
        elif k == 'host':
            if 'host' in ctx.params and ctx.params['host'] is not None:
                config_dict['client']['hosts'] = ctx.params[k]
        elif k in ['loglevel', 'logfile', 'logformat']:
            if k in ctx.params and ctx.params[k] is not None:
                config_dict['logging'][k] = ctx.params[k]
        else:
            if k in ctx.params and ctx.params[k] is not None:
                config_dict['client'][k] = ctx.params[k]
    # After override, prune the nones
    for k in ['client', 'logging']:
        config_dict[k] = prune_nones(config_dict[k])
    return SchemaCheck(config_dict, config_file.client(),
        'Client Configuration', 'full configuration dictionary').result()