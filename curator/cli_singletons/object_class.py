import logging
import sys
from curator import IndexList, SnapshotList
from curator.actions import (
    Alias, Allocation, Close, ClusterRouting, CreateIndex, DeleteIndices, DeleteSnapshots, ForceMerge,
    IndexSettings, Open, Reindex, Replicas, Restore, Rollover, Shrink, Snapshot
)
from curator.defaults.settings import snapshot_actions
from curator.exceptions import ConfigurationError, NoIndices, NoSnapshots
from curator.validators import SchemaCheck, filters, options
from curator.utils import get_client, prune_nones, validate_filters
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
    'index_settings' : IndexSettings,
    'open' : Open,
    'reindex' : Reindex,
    'replicas' : Replicas,
    'restore' : Restore,
    'rollover': Rollover,
    'shrink': Shrink,
    'snapshot' : Snapshot,
}

EXCLUDED_OPTIONS = [
    'ignore_empty_list', 'timeout_override',
    'continue_if_exception', 'disable_action'
]

class cli_action():
    """
    Unified class for all CLI singleton actions
    """
    def __init__(self, action, client_args, option_dict, filter_list, ignore_empty_list, **kwargs):
        self.logger = logging.getLogger('curator.cli_singletons.cli_action.' + action)
        self.action = action
        self.repository = kwargs['repository'] if 'repository' in kwargs else None
        if action[:5] != 'show_': # Ignore CLASS_MAP for show_indices/show_snapshots
            try:
                self.action_class = CLASS_MAP[action]
            except KeyError:
                self.logger.critical('Action must be one of {0}'.format(list(CLASS_MAP.keys())))
            self.check_options(option_dict)
        else:
            self.options = option_dict
        # Extract allow_ilm_indices so it can be handled separately.
        if 'allow_ilm_indices' in self.options:
            self.allow_ilm = self.options.pop('allow_ilm_indices')
        else:
            self.allow_ilm = False
        if action == 'alias':
            self.alias = {
                'name': option_dict['name'],
                'extra_settings': option_dict['extra_settings'],
                'wini': kwargs['warn_if_no_indices'] if 'warn_if_no_indices' in kwargs else False
            }
            for k in ['add', 'remove']:
                if k in kwargs:
                    self.alias[k] = {}
                    self.check_filters(kwargs[k], loc='alias singleton', key=k)
                    self.alias[k]['filters'] = self.filters
                    if self.allow_ilm:
                        self.alias[k]['filters'].append({'filtertype':'ilm'})
        elif action in [ 'cluster_routing', 'create_index', 'rollover']:
            self.action_kwargs = {}
            # No filters for these actions
            if action == 'rollover':
                # Ugh.  Look how I screwed up here with args instead of kwargs,
                # like EVERY OTHER ACTION seems to have... grr
                # todo: fix this in Curator 6, as it's an API-level change.
                self.action_args = (option_dict['name'], option_dict['conditions'])
                for k in ['new_index', 'extra_settings', 'wait_for_active_shards']:
                    self.action_kwargs[k] = kwargs[k] if k in kwargs else None
        else:
            self.check_filters(filter_list)
        self.client = get_client(**client_args)
        self.ignore = ignore_empty_list

    def prune_excluded(self, option_dict):
        for k in list(option_dict.keys()):
            if k in EXCLUDED_OPTIONS:
                del option_dict[k]
        return option_dict

    def check_options(self, option_dict):
        try:
            self.logger.debug('Validating provided options: {0}'.format(option_dict))
            # Kludgy work-around to needing 'repository' in options for these actions
            # but only to pass the schema check.  It's removed again below.
            if self.action in ['delete_snapshots', 'restore']:
                option_dict['repository'] = self.repository
            _ = SchemaCheck(
                prune_nones(option_dict),
                options.get_schema(self.action),
                'options',
                '{0} singleton action "options"'.format(self.action)
            ).result()
            self.options = self.prune_excluded(_)
            # Remove this after the schema check, as the action class won't need it as an arg
            if self.action in ['delete_snapshots', 'restore']:
                del self.options['repository']
        except ConfigurationError as e:
            self.logger.critical('Unable to parse options: {0}'.format(e))
            sys.exit(1)

    def check_filters(self, filter_dict, loc='singleton', key='filters'):
        try:
            self.logger.debug('Validating provided filters: {0}'.format(filter_dict))
            _ = SchemaCheck(
                filter_dict,
                Schema(filters.Filters(self.action, location=loc)),
                key,
                '{0} singleton action "{1}"'.format(self.action, key)
            ).result()
            self.filters = validate_filters(self.action, _)
        except ConfigurationError as e:
            self.logger.critical('Unable to parse filters: {0}'.format(e))
            sys.exit(1)

    def do_filters(self):
        self.logger.debug('Running filters and testing for empty list object')
        if self.allow_ilm:
            self.filters.append({'filtertype':'ilm','exclude':True})
        try:
            self.list_object.iterate_filters({'filters':self.filters})
            self.list_object.empty_list_check()
        except (NoIndices, NoSnapshots) as e:
            otype = 'index' if isinstance(e, NoIndices) else 'snapshot'
            if self.ignore:
                self.logger.info('Singleton action not performed: empty {0} list'.format(otype))
                sys.exit(0)
            else:
                self.logger.error('Singleton action failed due to empty {0} list'.format(otype))
                sys.exit(1)
    
    def get_list_object(self):
        if self.action in snapshot_actions() or self.action == 'show_snapshots':
            self.list_object = SnapshotList(self.client, repository=self.repository)
        else:
            self.list_object = IndexList(self.client)

    def get_alias_obj(self):
        action_obj = Alias(name=self.alias['name'], extra_settings=self.alias['extra_settings'])
        for k in ['remove', 'add']:
            if k in self.alias:
                self.logger.debug(
                    '{0}ing matching indices {1} alias "{2}"'.format(
                        'Add' if k == 'add' else 'Remov', # 0 = "Add" or "Remov"
                        'to' if k == 'add' else 'from', # 1 = "to" or "from"
                        self.alias['name'] # 2 = the alias name
                    )
                )
                self.alias[k]['ilo'] = IndexList(self.client)
                self.alias[k]['ilo'].iterate_filters({'filters':self.alias[k]['filters']})
                f = getattr(action_obj, k)
                f(self.alias[k]['ilo'], warn_if_no_indices=self.alias['wini'])
        return action_obj

    def do_singleton_action(self, dry_run=False):
        self.logger.debug('Doing the singleton "{0}" action here.'.format(self.action))
        try:
            if self.action == 'alias':
                action_obj = self.get_alias_obj()
            elif self.action in [ 'cluster_routing', 'create_index', 'rollover']:
                action_obj = self.action_class(self.client, *self.action_args, **self.action_kwargs)
            else:
                self.get_list_object()
                self.do_filters()
                self.logger.debug('OPTIONS = {0}'.format(self.options))
                action_obj = self.action_class(self.list_object, **self.options)
            try:
                if dry_run:
                    action_obj.do_dry_run()
                else:
                    action_obj.do_action()
            except Exception as e:
                raise e # pass it on?
        except Exception as e:
            self.logger.critical('Failed to complete action: {0}.  {1}: {2}'.format(self.action, type(e), e))
            sys.exit(1)
        self.logger.info('"{0}" action completed.'.format(self.action))