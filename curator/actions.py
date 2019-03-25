import logging
import re
import time
from copy import deepcopy
from datetime import datetime
from curator import exceptions, utils

class Alias(object):
    def __init__(self, name=None, extra_settings={}, **kwargs):
        """
        Define the Alias object.

        :arg name: The alias name
        :arg extra_settings: Extra settings, including filters and routing. For
            more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
        :type extra_settings: dict, representing the settings.
        """
        if not name:
            raise exceptions.MissingArgument('No value for "name" provided.')
        #: Instance variable
        #: The strftime parsed version of `name`.
        self.name = utils.parse_date_pattern(name)
        #: The list of actions to perform.  Populated by
        #: :mod:`curator.actions.Alias.add` and
        #: :mod:`curator.actions.Alias.remove`
        self.actions = []
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client  = None
        #: Instance variable.
        #: Any extra things to add to the alias, like filters, or routing.
        self.extra_settings = extra_settings
        self.loggit  = logging.getLogger('curator.actions.alias')
        #: Instance variable.
        #: Preset default value to `False`.
        self.warn_if_no_indices = False

    def add(self, ilo, warn_if_no_indices=False):
        """
        Create `add` statements for each index in `ilo` for `alias`, then
        append them to `actions`.  Add any `extras` that may be there.

        :arg ilo: A :class:`curator.indexlist.IndexList` object

        """
        utils.verify_index_list(ilo)
        if not self.client:
            self.client = ilo.client
        self.name = utils.parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except exceptions.NoIndices:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                self.loggit.warn(
                    'No indices found after processing filters. '
                    'Nothing to add to {0}'.format(self.name)
                )
                return
            else:
                # Re-raise the exceptions.NoIndices so it will behave as before
                raise exceptions.NoIndices('No indices to add to alias')
        for index in ilo.working_list():
            self.loggit.debug(
                'Adding index {0} to alias {1} with extra settings '
                '{2}'.format(index, self.name, self.extra_settings)
            )
            add_dict = { 'add' : { 'index' : index, 'alias': self.name } }
            add_dict['add'].update(self.extra_settings)
            self.actions.append(add_dict)

    def remove(self, ilo, warn_if_no_indices=False):
        """
        Create `remove` statements for each index in `ilo` for `alias`,
        then append them to `actions`.

        :arg ilo: A :class:`curator.indexlist.IndexList` object
        """
        utils.verify_index_list(ilo)
        if not self.client:
            self.client = ilo.client
        self.name = utils.parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except exceptions.NoIndices:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                self.loggit.warn(
                    'No indices found after processing filters. '
                    'Nothing to remove from {0}'.format(self.name)
                )
                return
            else:
                # Re-raise the exceptions.NoIndices so it will behave as before
                raise exceptions.NoIndices('No indices to remove from alias')
        aliases = self.client.indices.get_alias()
        for index in ilo.working_list():
            if index in aliases:
                self.loggit.debug(
                    'Index {0} in get_aliases output'.format(index))
                # Only remove if the index is associated with the alias
                if self.name in aliases[index]['aliases']:
                    self.loggit.debug(
                        'Removing index {0} from alias '
                        '{1}'.format(index, self.name)
                    )
                    self.actions.append(
                        { 'remove' : { 'index' : index, 'alias': self.name } })
                else:
                    self.loggit.debug(
                        'Can not remove: Index {0} is not associated with alias'
                        ' {1}'.format(index, self.name)
                    )

    def body(self):
        """
        Return a `body` string suitable for use with the `update_aliases` API
        call.
        """
        if not self.actions:
            if not self.warn_if_no_indices:
                raise exceptions.ActionError('No "add" or "remove" operations')
            else:
                raise exceptions.NoIndices('No "adds" or "removes" found.  Taking no action')
        self.loggit.debug('Alias actions: {0}'.format(self.actions))

        return { 'actions' : self.actions }

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        for item in self.body()['actions']:
            job = list(item.keys())[0]
            index = item[job]['index']
            alias = item[job]['alias']
            # We want our log to look clever, so if job is "remove", strip the
            # 'e' so "remove" can become "removing".  "adding" works already.
            self.loggit.info(
                'DRY-RUN: alias: {0}ing index "{1}" {2} alias '
                '"{3}"'.format(
                    job.rstrip('e'),
                    index,
                    'to' if job is 'add' else 'from',
                    alias
                )
            )

    def do_action(self):
        """
        Run the API call `update_aliases` with the results of `body()`
        """
        self.loggit.info('Updating aliases...')
        self.loggit.info('Alias actions: {0}'.format(self.body()))
        try:
            self.client.indices.update_aliases(body=self.body())
        except Exception as e:
            utils.report_failure(e)

class Allocation(object):
    def __init__(self, ilo, key=None, value=None, allocation_type='require',
        wait_for_completion=False, wait_interval=3, max_wait=-1,
        ):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg key: An arbitrary metadata attribute key.  Must match the key
            assigned to at least some of your nodes to have any effect.
        :arg value: An arbitrary metadata attribute value.  Must correspond to
            values associated with `key` assigned to at least some of your nodes
            to have any effect. If a `None` value is provided, it will remove
            any setting associated with that `key`.
        :arg allocation_type: Type of allocation to apply. Default is `require`
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `False`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`

        .. note::
            See:
            https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-allocation-filtering.html
        """
        utils.verify_index_list(ilo)
        if not key:
            raise exceptions.MissingArgument('No value for "key" provided')
        if allocation_type not in ['require', 'include', 'exclude']:
            raise ValueError(
                '{0} is an invalid allocation_type.  Must be one of "require", '
                '"include", "exclude".'.format(allocation_type)
            )
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client     = ilo.client
        self.loggit     = logging.getLogger('curator.actions.allocation')
        #: Instance variable.
        #: Populated at instance creation time. Value is
        #: ``index.routing.allocation.`` `allocation_type` ``.`` `key` ``.`` `value`
        bkey = 'index.routing.allocation.{0}.{1}'.format(allocation_type, key)
        self.body       = { bkey : value }
        #: Instance variable.
        #: Internal reference to `wait_for_completion`
        self.wfc        = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        utils.show_dry_run(self.index_list, 'allocation', body=self.body)

    def do_action(self):
        """
        Change allocation settings for indices in `index_list.indices` with the
        settings in `body`.
        """
        self.loggit.debug(
            'Cannot get change shard routing allocation of closed indices.  '
            'Omitting any closed indices.'
        )
        self.index_list.filter_closed()
        self.index_list.empty_list_check()
        self.loggit.info('Updating {0} selected indices: {1}'.format(len(self.index_list.indices), self.index_list.indices))
        self.loggit.info('Updating index setting {0}'.format(self.body))
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                self.client.indices.put_settings(
                    index=utils.to_csv(l), body=self.body
                )
                if self.wfc:
                    self.loggit.debug(
                        'Waiting for shards to complete relocation for indices:'
                        ' {0}'.format(utils.to_csv(l))
                    )
                    utils.wait_for_it(
                        self.client, 'allocation',
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
        except Exception as e:
            utils.report_failure(e)

class Close(object):
    def __init__(self, ilo, delete_aliases=False):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg delete_aliases: If `True`, will delete any associated aliases
            before closing indices.
        :type delete_aliases: bool
        """
        utils.verify_index_list(ilo)
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internal reference to `delete_aliases`
        self.delete_aliases = delete_aliases
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client     = ilo.client
        self.loggit     = logging.getLogger('curator.actions.close')


    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        utils.show_dry_run(
            self.index_list, 'close', **{'delete_aliases':self.delete_aliases})

    def do_action(self):
        """
        Close open indices in `index_list.indices`
        """
        self.index_list.filter_closed()
        self.index_list.empty_list_check()
        self.loggit.info(
            'Closing {0} selected indices: {1}'.format(len(self.index_list.indices), self.index_list.indices))
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                if self.delete_aliases:
                    self.loggit.info(
                        'Deleting aliases from indices before closing.')
                    self.loggit.debug('Deleting aliases from: {0}'.format(l))
                    try:
                        self.client.indices.delete_alias(
                            index=utils.to_csv(l), name='_all')
                    except Exception as e:
                        self.loggit.warn(
                            'Some indices may not have had aliases.  Exception:'
                            ' {0}'.format(e)
                        )
                self.client.indices.flush_synced(
                    index=utils.to_csv(l), ignore_unavailable=True)
                self.client.indices.close(
                    index=utils.to_csv(l), ignore_unavailable=True)
        except Exception as e:
            utils.report_failure(e)

class ClusterRouting(object):
    def __init__(
        self, client, routing_type=None, setting=None, value=None,
        wait_for_completion=False, wait_interval=9, max_wait=-1
    ):
        """
        For now, the cluster routing settings are hardcoded to be ``transient``

        :arg client: An :class:`elasticsearch.Elasticsearch` client object
        :arg routing_type: Type of routing to apply. Either `allocation` or
            `rebalance`
        :arg setting: Currently, the only acceptable value for `setting` is
            ``enable``. This is here in case that changes.
        :arg value: Used only if `setting` is `enable`. Semi-dependent on
            `routing_type`. Acceptable values for `allocation` and `rebalance`
            are ``all``, ``primaries``, and ``none`` (string, not `NoneType`).
            If `routing_type` is `allocation`, this can also be
            ``new_primaries``, and if `rebalance`, it can be ``replicas``.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `False`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        """
        utils.verify_client_object(client)
        #: Instance variable.
        #: An :class:`elasticsearch.Elasticsearch` client object
        self.client  = client
        self.loggit  = logging.getLogger('curator.actions.cluster_routing')
        #: Instance variable.
        #: Internal reference to `wait_for_completion`
        self.wfc     = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait

        if setting != 'enable':
            raise ValueError(
                'Invalid value for "setting": {0}.'.format(setting)
            )
        if routing_type == 'allocation':
            if value not in ['all', 'primaries', 'new_primaries', 'none']:
                raise ValueError(
                    'Invalid "value": {0} with "routing_type":'
                    '{1}.'.format(value, routing_type)
                )
        elif routing_type == 'rebalance':
            if value not in ['all', 'primaries', 'replicas', 'none']:
                raise ValueError(
                    'Invalid "value": {0} with "routing_type":'
                    '{1}.'.format(value, routing_type)
                )
        else:
            raise ValueError(
                'Invalid value for "routing_type": {0}.'.format(routing_type)
            )
        bkey = 'cluster.routing.{0}.{1}'.format(routing_type,setting)
        self.body = { 'transient' : { bkey : value } }

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.loggit.info(
            'DRY-RUN: Update cluster routing settings with arguments: '
            '{0}'.format(self.body)
        )

    def do_action(self):
        """
        Change cluster routing settings with the settings in `body`.
        """
        self.loggit.info('Updating cluster settings: {0}'.format(self.body))
        try:
            self.client.cluster.put_settings(body=self.body)
            if self.wfc:
                self.loggit.debug(
                    'Waiting for shards to complete routing and/or rebalancing'
                )
                utils.wait_for_it(
                    self.client, 'cluster_routing',
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
        except Exception as e:
            utils.report_failure(e)

class CreateIndex(object):
    def __init__(self, client, name, extra_settings={}):
        """
        :arg client: An :class:`elasticsearch.Elasticsearch` client object
        :arg name: A name, which can contain :py:func:`time.strftime`
            strings
        :arg extra_settings: The `settings` and `mappings` for the index. For
            more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
        :type extra_settings: dict, representing the settings and mappings.
        """
        if not name:
            raise exceptions.ConfigurationError('Value for "name" not provided.')
        #: Instance variable.
        #: The parsed version of `name`
        self.name       = utils.parse_date_pattern(name)
        #: Instance variable.
        #: Extracted from the config yaml, it should be a dictionary of
        #: mappings and settings suitable for index creation.
        self.body       = extra_settings
        #: Instance variable.
        #: An :class:`elasticsearch.Elasticsearch` client object
        self.client     = client
        self.loggit     = logging.getLogger('curator.actions.create_index')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.loggit.info(
            'DRY-RUN: create_index "{0}" with arguments: '
            '{1}'.format(self.name, self.body)
        )

    def do_action(self):
        """
        Create index identified by `name` with settings in `body`
        """
        self.loggit.info(
            'Creating index "{0}" with settings: '
            '{1}'.format(self.name, self.body)
        )
        try:
            self.client.indices.create(index=self.name, body=self.body)
        except Exception as e:
            utils.report_failure(e)

class DeleteIndices(object):
    def __init__(self, ilo, master_timeout=30):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg master_timeout: Number of seconds to wait for master node response
        """
        utils.verify_index_list(ilo)
        if not isinstance(master_timeout, int):
            raise TypeError(
                'Incorrect type for "master_timeout": {0}. '
                'Should be integer value.'.format(type(master_timeout))
            )
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list     = ilo
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client         = ilo.client
        #: Instance variable.
        #: String value of `master_timeout` + 's', for seconds.
        self.master_timeout = str(master_timeout) + 's'
        self.loggit         = logging.getLogger('curator.actions.delete_indices')
        self.loggit.debug('master_timeout value: {0}'.format(
            self.master_timeout))

    def _verify_result(self, result, count):
        """
        Breakout method to aid readability
        :arg result: A list of indices from `_get_result_list`
        :arg count: The number of tries that have occurred
        :rtype: bool
        """
        if len(result) > 0:
            self.loggit.error(
                'The following indices failed to delete on try '
                '#{0}:'.format(count)
            )
            for idx in result:
                self.loggit.error("---{0}".format(idx))
            return False
        else:
            self.loggit.debug(
                'Successfully deleted all indices on try #{0}'.format(count)
            )
            return True

    def __chunk_loop(self, chunk_list):
        """
        Loop through deletes 3 times to ensure they complete
        :arg chunk_list: A list of indices pre-chunked so it won't overload the
            URL size limit.
        """
        working_list = chunk_list
        for count in range(1, 4): # Try 3 times
            for i in working_list:
                self.loggit.info("---deleting index {0}".format(i))
            self.client.indices.delete(
                index=utils.to_csv(working_list), master_timeout=self.master_timeout)
            result = [ i for i in working_list if i in utils.get_indices(self.client)]
            if self._verify_result(result, count):
                return
            else:
                working_list = result
        self.loggit.error(
            'Unable to delete the following indices after 3 attempts: '
            '{0}'.format(result)
        )

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        utils.show_dry_run(self.index_list, 'delete_indices')

    def do_action(self):
        """
        Delete indices in `index_list.indices`
        """
        self.index_list.empty_list_check()
        self.loggit.info(
            'Deleting {0} selected indices: {1}'.format(len(self.index_list.indices), self.index_list.indices))
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                self.__chunk_loop(l)
        except Exception as e:
            utils.report_failure(e)

class ForceMerge(object):
    def __init__(self, ilo, max_num_segments=None, delay=0):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg max_num_segments: Number of segments per shard to forceMerge
        :arg delay: Number of seconds to delay between forceMerge operations
        """
        utils.verify_index_list(ilo)
        if not max_num_segments:
            raise exceptions.MissingArgument('Missing value for "max_num_segments"')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `max_num_segments`
        self.max_num_segments = max_num_segments
        #: Instance variable.
        #: Internally accessible copy of `delay`
        self.delay = delay
        self.loggit = logging.getLogger('curator.actions.forcemerge')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        utils.show_dry_run(
            self.index_list, 'forcemerge',
            max_num_segments=self.max_num_segments,
            delay=self.delay,
        )

    def do_action(self):
        """
        forcemerge indices in `index_list.indices`
        """
        self.index_list.filter_closed()
        self.index_list.filter_forceMerged(
            max_num_segments=self.max_num_segments)
        self.index_list.empty_list_check()
        self.loggit.info('forceMerging {0} selected indices: {1}'.format(len(self.index_list.indices), self.index_list.indices))
        try:
            for index_name in self.index_list.indices:
                self.loggit.info(
                    'forceMerging index {0} to {1} segments per shard.  '
                    'Please wait...'.format(index_name, self.max_num_segments)
                )
                self.client.indices.forcemerge(index=index_name,
                    max_num_segments=self.max_num_segments)
                if self.delay > 0:
                    self.loggit.info(
                        'Pausing for {0} seconds before continuing...'.format(
                            self.delay)
                    )
                    time.sleep(self.delay)
        except Exception as e:
            utils.report_failure(e)

class IndexSettings(object):
    def __init__(self, ilo, index_settings={}, ignore_unavailable=False,
        preserve_existing=False):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg index_settings: A dictionary structure with one or more index
            settings to change.
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg preserve_existing: Whether to update existing settings. If set to
            ``True`` existing settings on an index remain unchanged. The default
            is ``False``
        """
        utils.verify_index_list(ilo)
        if not index_settings:
            raise exceptions.MissingArgument('Missing value for "index_settings"')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client     = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internal reference to `index_settings`
        self.body = index_settings
        #: Instance variable.
        #: Internal reference to `ignore_unavailable`
        self.ignore_unavailable = ignore_unavailable
        #: Instance variable.
        #: Internal reference to `preserve_settings`
        self.preserve_existing = preserve_existing

        self.loggit     = logging.getLogger('curator.actions.index_settings')
        self._body_check()

    def _body_check(self):
        # The body only passes the skimpiest of requirements by having 'index'
        # as the only root-level key, and having a 'dict' as its value
        if len(self.body) == 1:
            if 'index' in self.body:
                if isinstance(self.body['index'], dict):
                    return True
        raise exceptions.ConfigurationError(
            'Bad value for "index_settings": {0}'.format(self.body))

    def _static_settings(self):
        return [
            'number_of_shards',
            'shard',
            'codec',
            'routing_partition_size',
        ]

    def _dynamic_settings(self):
        return [
            'number_of_replicas',
            'auto_expand_replicas',
            'refresh_interval',
            'max_result_window',
            'max_rescore_window',
            'blocks',
            'max_refresh_listeners',
            'mapping',
            'merge',
            'translog',
        ]

    def _settings_check(self):
        # Detect if even one index is open.  Save all found to open_index_list.
        open_index_list = []
        open_indices = False
        for idx in self.index_list.indices:
            if self.index_list.index_info[idx]['state'] == 'open':
                open_index_list.append(idx)
                open_indices = True
        for k in self.body['index']:
            if k in self._static_settings():
                if not self.ignore_unavailable:
                    if open_indices:
                        raise exceptions.ActionError(
                            'Static Setting "{0}" detected with open indices: '
                            '{1}. Static settings can only be used with closed '
                            'indices.  Recommend filtering out open indices, '
                            'or setting ignore_unavailable to True'.format(
                                k, open_index_list
                            )
                        )
            elif k in self._dynamic_settings():
                # Dynamic settings should be appliable to open or closed indices
                # Act here if the case is different for some settings.
                pass
            else:
                self.loggit.warn(
                    '"{0}" is not a setting Curator recognizes and may or may '
                    'not work.'.format(k)
                )

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        utils.show_dry_run(self.index_list, 'indexsettings', **self.body)

    def do_action(self):
        self._settings_check()
        # Ensure that the open indices filter applied in _settings_check()
        # didn't result in an empty list (or otherwise empty)
        self.index_list.empty_list_check()
        self.loggit.info(
            'Applying index settings to {0} indices: '
            '{1}'.format(len(self.index_list.indices), self.index_list.indices)
        )
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                response = self.client.indices.put_settings(
                    index=utils.to_csv(l), body=self.body,
                    ignore_unavailable=self.ignore_unavailable,
                    preserve_existing=self.preserve_existing
                )
                self.loggit.debug('PUT SETTINGS RESPONSE: {0}'.format(response))
        except Exception as e:
            utils.report_failure(e)


class Open(object):
    def __init__(self, ilo):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        """
        utils.verify_index_list(ilo)
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client     = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        self.loggit     = logging.getLogger('curator.actions.open')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        utils.show_dry_run(self.index_list, 'open')

    def do_action(self):
        """
        Open closed indices in `index_list.indices`
        """
        self.index_list.empty_list_check()
        self.loggit.info(
            'Opening {0} selected indices: {1}'.format(len(self.index_list.indices), self.index_list.indices))
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                self.client.indices.open(index=utils.to_csv(l))
        except Exception as e:
            utils.report_failure(e)

class Replicas(object):
    def __init__(self, ilo, count=None, wait_for_completion=False,
        wait_interval=9, max_wait=-1):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg count: The count of replicas per shard
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `False`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        """
        utils.verify_index_list(ilo)
        # It's okay for count to be zero
        if count == 0:
            pass
        elif not count:
            raise exceptions.MissingArgument('Missing value for "count"')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client     = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `count`
        self.count      = count
        #: Instance variable.
        #: Internal reference to `wait_for_completion`
        self.wfc        = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        self.loggit     = logging.getLogger('curator.actions.replicas')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        utils.show_dry_run(self.index_list, 'replicas', count=self.count)

    def do_action(self):
        """
        Update the replica count of indices in `index_list.indices`
        """
        self.loggit.debug(
            'Cannot get update replica count of closed indices.  '
            'Omitting any closed indices.'
        )
        self.index_list.filter_closed()
        self.index_list.empty_list_check()
        self.loggit.info(
            'Setting the replica count to {0} for {1} indices: '
            '{2}'.format(self.count, len(self.index_list.indices), self.index_list.indices)
        )
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                self.client.indices.put_settings(index=utils.to_csv(l),
                    body={'number_of_replicas' : self.count})
                if self.wfc and self.count > 0:
                    self.loggit.debug(
                        'Waiting for shards to complete replication for '
                        'indices: {0}'.format(utils.to_csv(l))
                    )
                    utils.wait_for_it(
                        self.client, 'replicas',
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
        except Exception as e:
            utils.report_failure(e)

class Rollover(object):
    def __init__(
            self, client, name, conditions, new_index=None, extra_settings=None,
            wait_for_active_shards=1
        ):
        """
        :arg client: An :class:`elasticsearch.Elasticsearch` client object
        :arg name: The name of the single-index-mapped alias to test for
            rollover conditions.
        :new_index: The new index name
        :arg conditions: A dictionary of conditions to test
        :arg extra_settings: Must be either `None`, or a dictionary of settings
            to apply to the new index on rollover. This is used in place of
            `settings` in the Rollover API, mostly because it's already existent
            in other places here in Curator
        :arg wait_for_active_shards: The number of shards expected to be active
            before returning.
        """
        self.loggit     = logging.getLogger('curator.actions.rollover')
        if not isinstance(conditions, dict):
            raise exceptions.ConfigurationError('"conditions" must be a dictionary')
        else:
            self.loggit.debug('"conditions" is {0}'.format(conditions))
        if not isinstance(extra_settings, dict) and extra_settings is not None:
            raise exceptions.ConfigurationError(
                '"extra_settings" must be a dictionary or None')
        utils.verify_client_object(client)
        #: Instance variable.
        #: The Elasticsearch Client object
        self.client = client
        #: Instance variable.
        #: Internal reference to `conditions`
        self.conditions = self._check_max_size(conditions)
        #: Instance variable.
        #: Internal reference to `extra_settings`
        self.settings = extra_settings
        #: Instance variable.
        #: Internal reference to `new_index`
        self.new_index = utils.parse_date_pattern(new_index) if new_index else new_index
        #: Instance variable.
        #: Internal reference to `wait_for_active_shards`
        self.wait_for_active_shards = wait_for_active_shards

        # Verify that `conditions` and `settings` are good?
        # Verify that `name` is an alias, and is only mapped to one index.
        if utils.rollable_alias(client, name):
            self.name = name
        else:
            raise ValueError(
                    'Unable to perform index rollover with alias '
                    '"{0}". See previous logs for more details.'.format(name)
                )

    def _check_max_size(self, conditions):
        """
        Ensure that if ``max_size`` is specified, that ``self.client``
        is running 6.1 or higher.
        """
        if 'max_size' in conditions:
            version = utils.get_version(self.client)
            if version < (6,1,0):
                raise exceptions.ConfigurationError(
                    'Your version of elasticsearch ({0}) does not support '
                    'the max_size rollover condition. It is only supported '
                    'in versions 6.1.0 and up.'.format(version)
                )
        return conditions

    def body(self):
        """
        Create a body from conditions and settings
        """
        retval = {}
        retval['conditions'] = self.conditions
        if self.settings:
            retval['settings'] = self.settings
        return retval

    def log_result(self, result):
        """
        Log the results based on whether the index rolled over or not
        """
        dryrun_string = ''
        if result['dry_run']:
            dryrun_string = 'DRY-RUN: '
        self.loggit.debug('{0}Result: {1}'.format(dryrun_string, result))
        rollover_string = '{0}Old index {1} rolled over to new index {2}'.format(
            dryrun_string,
            result['old_index'],
            result['new_index']
        )
        # Success is determined by at one condition being True
        success = False
        for k in list(result['conditions'].keys()):
            if result['conditions'][k]:
                success = True
        if result['dry_run'] and success: # log "successful" dry-run
            self.loggit.info(rollover_string)
        elif result['rolled_over']:
            self.loggit.info(rollover_string)
        else:
            self.loggit.info(
                '{0}Rollover conditions not met. Index {0} not rolled over.'.format(
                    dryrun_string,
                    result['old_index'])
            )

    def doit(self, dry_run=False):
        """
        This exists solely to prevent having to have duplicate code in both
        `do_dry_run` and `do_action`
        """
        return self.client.indices.rollover(
            alias=self.name,
            new_index=self.new_index,
            body=self.body(),
            dry_run=dry_run,
            wait_for_active_shards=self.wait_for_active_shards,
        )

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.log_result(self.doit(dry_run=True))

    def do_action(self):
        """
        Rollover the index referenced by alias `name`
        """
        self.loggit.info('Performing index rollover')
        try:
            self.log_result(self.doit())
        except Exception as e:
            utils.report_failure(e)

class DeleteSnapshots(object):
    def __init__(self, slo, retry_interval=120, retry_count=3):
        """
        :arg slo: A :class:`curator.snapshotlist.SnapshotList` object
        :arg retry_interval: Number of seconds to delay betwen retries. Default:
            120 (seconds)
        :arg retry_count: Number of attempts to make. Default: 3
        """
        utils.verify_snapshot_list(slo)
        #: Instance variable.
        #: The Elasticsearch Client object derived from `slo`
        self.client         = slo.client
        #: Instance variable.
        #: Internally accessible copy of `retry_interval`
        self.retry_interval = retry_interval
        #: Instance variable.
        #: Internally accessible copy of `retry_count`
        self.retry_count    = retry_count
        #: Instance variable.
        #: Internal reference to `slo`
        self.snapshot_list  = slo
        #: Instance variable.
        #: The repository name derived from `slo`
        self.repository     = slo.repository
        self.loggit = logging.getLogger('curator.actions.delete_snapshots')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        mykwargs = {
            'repository' : self.repository,
            'retry_interval' : self.retry_interval,
            'retry_count' : self.retry_count,
        }
        for snap in self.snapshot_list.snapshots:
            self.loggit.info('DRY-RUN: delete_snapshot: {0} with arguments: '
                '{1}'.format(snap, mykwargs))

    def do_action(self):
        """
        Delete snapshots in `slo`
        Retry up to `retry_count` times, pausing `retry_interval`
        seconds between retries.
        """
        self.snapshot_list.empty_list_check()
        self.loggit.info('Deleting {0} selected snapshots: {1}'.format(len(self.snapshot_list.snapshots), self.snapshot_list.snapshots))
        if not utils.safe_to_snap(
            self.client, repository=self.repository,
            retry_interval=self.retry_interval, retry_count=self.retry_count):
                raise exceptions.FailedExecution(
                    'Unable to delete snapshot(s) because a snapshot is in '
                    'state "IN_PROGRESS"')
        try:
            for s in self.snapshot_list.snapshots:
                self.loggit.info('Deleting snapshot {0}...'.format(s))
                self.client.snapshot.delete(
                    repository=self.repository, snapshot=s)
        except Exception as e:
            utils.report_failure(e)

class Reindex(object):
    def __init__(self, ilo, request_body, refresh=True,
        requests_per_second=-1, slices=1, timeout=60, wait_for_active_shards=1,
        wait_for_completion=True, max_wait=-1, wait_interval=9,
        remote_url_prefix=None, remote_ssl_no_validate=None,
        remote_certificate=None, remote_client_cert=None,
        remote_client_key=None, remote_aws_key=None, remote_aws_secret_key=None,
        remote_aws_region=None, remote_filters={}, migration_prefix='',
        migration_suffix=''):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg request_body: The body to send to
            :py:meth:`elasticsearch.Elasticsearch.reindex`, which must be complete and
            usable, as Curator will do no vetting of the request_body. If it
            fails to function, Curator will return an exception.
        :arg refresh: Whether to refresh the entire target index after the
            operation is complete. (default: `True`)
        :type refresh: bool
        :arg requests_per_second: The throttle to set on this request in
            sub-requests per second. ``-1`` means set no throttle as does
            ``unlimited`` which is the only non-float this accepts. (default:
            ``-1``)
        :arg slices: The number of slices this task  should be divided into. 1
            means the task will not be sliced into subtasks. (default: ``1``)
        :arg timeout: The length in seconds each individual bulk request should
            wait for shards that are unavailable. (default: ``60``)
        :arg wait_for_active_shards: Sets the number of shard copies that must
            be active before proceeding with the reindex operation. (default:
            ``1``) means the primary shard only. Set to ``all`` for all shard
            copies, otherwise set to any non-negative value less than or equal
            to the total number of copies for the shard (number of replicas + 1)
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :arg remote_url_prefix: `Optional` url prefix, if needed to reach the
            Elasticsearch API (i.e., it's not at the root level)
        :type remote_url_prefix: str
        :arg remote_ssl_no_validate: If `True`, do not validate the certificate
            chain.  This is an insecure option and you will see warnings in the
            log output.
        :type remote_ssl_no_validate: bool
        :arg remote_certificate: Path to SSL/TLS certificate
        :arg remote_client_cert: Path to SSL/TLS client certificate (public key)
        :arg remote_client_key: Path to SSL/TLS private key
        :arg remote_aws_key: AWS IAM Access Key (Only used if the
            :mod:`requests-aws4auth` python module is installed)
        :arg remote_aws_secret_key: AWS IAM Secret Access Key (Only used if the
            :mod:`requests-aws4auth` python module is installed)
        :arg remote_aws_region: AWS Region (Only used if the
            :mod:`requests-aws4auth` python module is installed)
        :arg remote_filters: Apply these filters to the remote client for
            remote index selection.
        :arg migration_prefix: When migrating, prepend this value to the index
            name.
        :arg migration_suffix: When migrating, append this value to the index
            name.
        """
        self.loggit = logging.getLogger('curator.actions.reindex')
        utils.verify_index_list(ilo)
        # Normally, we'd check for an empty list here.  But since we can reindex
        # from remote, we might just be starting with an empty one.
        # ilo.empty_list_check()
        if not isinstance(request_body, dict):
            raise exceptions.ConfigurationError('"request_body" is not of type dictionary')
        #: Instance variable.
        #: Internal reference to `request_body`
        self.body = request_body
        self.loggit.debug('REQUEST_BODY = {0}'.format(request_body))
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internal reference to `refresh`
        self.refresh = refresh
        #: Instance variable.
        #: Internal reference to `requests_per_second`
        self.requests_per_second = requests_per_second
        #: Instance variable.
        #: Internal reference to `slices`
        self.slices = slices
        #: Instance variable.
        #: Internal reference to `timeout`, and add "s" for seconds.
        self.timeout = '{0}s'.format(timeout)
        #: Instance variable.
        #: Internal reference to `wait_for_active_shards`
        self.wait_for_active_shards = wait_for_active_shards
        #: Instance variable.
        #: Internal reference to `wait_for_completion`
        self.wfc = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        #: Instance variable.
        #: Internal reference to `migration_prefix`
        self.mpfx = migration_prefix
        #: Instance variable.
        #: Internal reference to `migration_suffix`
        self.msfx = migration_suffix

        # This is for error logging later...
        self.remote = False
        if 'remote' in self.body['source']:
            self.remote = True

        self.migration = False
        if self.body['dest']['index'] == 'MIGRATION':
            self.migration = True

        if self.migration:
            if not self.remote and not self.mpfx and not self.msfx:
                raise exceptions.ConfigurationError(
                    'MIGRATION can only be used locally with one or both of '
                    'migration_prefix or migration_suffix.'
                )

        # REINDEX_SELECTION is the designated token.  If you use this for the
        # source "index," it will be replaced with the list of indices from the
        # provided 'ilo' (index list object).
        if self.body['source']['index'] == 'REINDEX_SELECTION' \
                and not self.remote:
            self.body['source']['index'] = self.index_list.indices

        # Remote section
        elif self.remote:
            self.loggit.debug('Remote reindex request detected')
            if 'host' not in self.body['source']['remote']:
                raise exceptions.ConfigurationError('Missing remote "host"')
            rclient_info = {}
            for k in ['host', 'username', 'password']:
                rclient_info[k] = self.body['source']['remote'][k] \
                    if k in self.body['source']['remote'] else None
            rhost = rclient_info['host']
            try:
                # Save these for logging later
                a = rhost.split(':')
                self.remote_port = a[2]
                self.remote_host = a[1][2:]
            except Exception as e:
                raise exceptions.ConfigurationError(
                    'Host must be in the form [scheme]://[host]:[port] but '
                    'was [{0}]'.format(rhost)
                )
            rhttp_auth = '{0}:{1}'.format(
                    rclient_info['username'],rclient_info['password']) \
                if (rclient_info['username'] and rclient_info['password']) \
                    else None
            if rhost[:5] == 'http:':
                use_ssl = False
            elif rhost[:5] == 'https':
                use_ssl = True
            else:
                raise exceptions.ConfigurationError(
                    'Host must be in URL format. You provided: '
                    '{0}'.format(rclient_info['host'])
                )

            # Let's set a decent remote timeout for initially reading
            # the indices on the other side, and collecting their metadata
            remote_timeout = 180

            # The rest only applies if using filters for remote indices
            if self.body['source']['index'] == 'REINDEX_SELECTION':
                self.loggit.debug('Filtering indices from remote')
                from .indexlist import IndexList
                self.loggit.debug('Remote client args: '
                    'host={0} '
                    'http_auth={1} '
                    'url_prefix={2} '
                    'use_ssl={3} '
                    'ssl_no_validate={4} '
                    'certificate={5} '
                    'client_cert={6} '
                    'client_key={7} '
                    'aws_key={8} '
                    'aws_secret_key={9} '
                    'aws_region={10} '
                    'timeout={11} '
                    'skip_version_test=True'.format(
                        rhost,
                        rhttp_auth,
                        remote_url_prefix,
                        use_ssl,
                        remote_ssl_no_validate,
                        remote_certificate,
                        remote_client_cert,
                        remote_client_key,
                        remote_aws_key,
                        remote_aws_secret_key,
                        remote_aws_region,
                        remote_timeout
                    )
                )

                try: # let's try to build a remote connection with these!
                    rclient = utils.get_client(
                        host=rhost,
                        http_auth=rhttp_auth,
                        url_prefix=remote_url_prefix,
                        use_ssl=use_ssl,
                        ssl_no_validate=remote_ssl_no_validate,
                        certificate=remote_certificate,
                        client_cert=remote_client_cert,
                        client_key=remote_client_key,
                        aws_key=remote_aws_key,
                        aws_secret_key=remote_aws_secret_key,
                        aws_region=remote_aws_region,
                        skip_version_test=True,
                        timeout=remote_timeout
                    )
                except Exception as e:
                    self.loggit.error(
                        'Unable to establish connection to remote Elasticsearch'
                        ' with provided credentials/certificates/settings.'
                    )
                    utils.report_failure(e)
                try:
                    rio = IndexList(rclient)
                    rio.iterate_filters({'filters': remote_filters})
                    try:
                        rio.empty_list_check()
                    except exceptions.NoIndices:
                        raise exceptions.FailedExecution(
                            'No actionable remote indices selected after '
                            'applying filters.'
                        )
                    self.body['source']['index'] = rio.indices
                except Exception as e:
                    self.loggit.error(
                        'Unable to get/filter list of remote indices.'
                    )
                    utils.report_failure(e)

        self.loggit.debug(
            'Reindexing indices: {0}'.format(self.body['source']['index']))

    def _get_request_body(self, source, dest):
        body = deepcopy(self.body)
        body['source']['index'] = source
        body['dest']['index'] = dest
        return body

    def _get_reindex_args(self, source, dest):
        # Always set wait_for_completion to False. Let 'utils.wait_for_it' do its
        # thing if wait_for_completion is set to True. Report the task_id
        # either way.
        reindex_args = {
            'body':self._get_request_body(source, dest), 'refresh':self.refresh,
            'requests_per_second': self.requests_per_second,
            'timeout': self.timeout,
            'wait_for_active_shards': self.wait_for_active_shards,
            'wait_for_completion': False,
            'slices': self.slices
        }
        version = utils.get_version(self.client)
        if version < (5,1,0):
            self.loggit.info(
                'Your version of elasticsearch ({0}) does not support '
                'sliced scroll for reindex, so that setting will not be '
                'used'.format(version)
            )
            del reindex_args['slices']
        return reindex_args

    def _post_run_quick_check(self, index_name):
        # Verify the destination index is there after the fact
        index_exists = self.client.indices.exists(index=index_name)
        alias_instead = self.client.indices.exists_alias(name=index_name)
        if not index_exists and not alias_instead:
            self.loggit.error(
                'The index described as "{0}" was not found after the reindex '
                'operation. Check Elasticsearch logs for more '
                'information.'.format(index_name)
            )
            if self.remote:
                self.loggit.error(
                    'Did you forget to add "reindex.remote.whitelist: '
                    '{0}:{1}" to the elasticsearch.yml file on the '
                    '"dest" node?'.format(
                        self.remote_host, self.remote_port
                    )
                )
            raise exceptions.FailedExecution(
                'Reindex failed. The index or alias identified by "{0}" was '
                'not found.'.format(index_name)
            )

    def sources(self):
        # Generator for sources & dests
        dest = self.body['dest']['index']
        source_list = utils.ensure_list(self.body['source']['index'])
        self.loggit.debug('source_list: {0}'.format(source_list))
        if source_list == [] or source_list == ['REINDEX_SELECTED']: # Empty list
            raise exceptions.NoIndices
        if not self.migration:
            yield self.body['source']['index'], dest

        # Loop over all sources (default will only be one)
        else:
            for source in source_list:
                if self.migration:
                    dest = self.mpfx + source + self.msfx
                yield source, dest

    def show_run_args(self, source, dest):
        """
        Show what will run
        """

        return ('request body: {0} with arguments: '
            'refresh={1} '
            'requests_per_second={2} '
            'slices={3} '
            'timeout={4} '
            'wait_for_active_shards={5} '
            'wait_for_completion={6}'.format(
                self._get_request_body(source, dest),
                self.refresh,
                self.requests_per_second,
                self.slices,
                self.timeout,
                self.wait_for_active_shards,
                self.wfc
            )
        )

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        for source, dest in self.sources():
            self.loggit.info(
                'DRY-RUN: REINDEX: {0}'.format(self.show_run_args(source, dest))
            )

    def do_action(self):
        """
        Execute :py:meth:`elasticsearch.Elasticsearch.reindex` operation with the
        provided request_body and arguments.
        """
        try:
            # Loop over all sources (default will only be one)
            for source, dest in self.sources():
                self.loggit.info('Commencing reindex operation')
                self.loggit.debug(
                    'REINDEX: {0}'.format(self.show_run_args(source, dest)))
                response = self.client.reindex(
                                **self._get_reindex_args(source, dest))

                self.loggit.debug('TASK ID = {0}'.format(response['task']))
                if self.wfc:
                    utils.wait_for_it(
                        self.client, 'reindex', task_id=response['task'],
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
                    self._post_run_quick_check(dest)

                else:
                    self.loggit.warn(
                        '"wait_for_completion" set to {0}.  Remember '
                        'to check task_id "{1}" for successful completion '
                        'manually.'.format(self.wfc, response['task'])
                    )
        except exceptions.NoIndices as e:
            raise exceptions.NoIndices(
                'Source index must be list of actual indices. '
                'It must not be an empty list.')
        except Exception as e:
            utils.report_failure(e)


class Snapshot(object):
    def __init__(self, ilo, repository=None, name=None,
                ignore_unavailable=False, include_global_state=True,
                partial=False, wait_for_completion=True, wait_interval=9,
                max_wait=-1, skip_repo_fs_check=False):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg repository: The Elasticsearch snapshot repository to use
        :arg name: What to name the snapshot.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :arg ignore_unavailable: Ignore unavailable shards/indices.
            (default: `False`)
        :type ignore_unavailable: bool
        :arg include_global_state: Store cluster global state with snapshot.
            (default: `True`)
        :type include_global_state: bool
        :arg partial: Do not fail if primary shard is unavailable. (default:
            `False`)
        :type partial: bool
        :arg skip_repo_fs_check: Do not validate write access to repository on
            all cluster nodes before proceeding. (default: `False`).  Useful for
            shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success.
        :type skip_repo_fs_check: bool
        """
        utils.verify_index_list(ilo)
        # Check here and don't bother with the rest of this if there are no
        # indices in the index list.
        ilo.empty_list_check()
        if not utils.repository_exists(ilo.client, repository=repository):
            raise exceptions.ActionError(
                'Cannot snapshot indices to missing repository: '
                '{0}'.format(repository)
            )
        if not name:
            raise exceptions.MissingArgument('No value for "name" provided.')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client              = ilo.client
        #: Instance variable.
        #: The parsed version of `name`
        self.name = utils.parse_datemath(self.client, utils.parse_date_pattern(name))
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `repository`
        self.repository          = repository
        #: Instance variable.
        #: Internally accessible copy of `wait_for_completion`
        self.wait_for_completion = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        #: Instance variable.
        #: Internally accessible copy of `skip_repo_fs_check`
        self.skip_repo_fs_check  = skip_repo_fs_check
        self.state               = None

        #: Instance variable.
        #: Populated at instance creation time by calling
        #: :mod:`curator.utils.utils.create_snapshot_body` with `ilo.indices` and the
        #: provided arguments: `ignore_unavailable`, `include_global_state`,
        #: `partial`
        self.body                = utils.create_snapshot_body(
                ilo.indices,
                ignore_unavailable=ignore_unavailable,
                include_global_state=include_global_state,
                partial=partial
            )

        self.loggit = logging.getLogger('curator.actions.snapshot')

    def get_state(self):
        """
        Get the state of the snapshot
        """
        try:
            self.state = self.client.snapshot.get(
                repository=self.repository,
                snapshot=self.name)['snapshots'][0]['state']
            return self.state
        except IndexError:
            raise exceptions.CuratorException(
                'Snapshot "{0}" not found in repository '
                '"{1}"'.format(self.name, self.repository)
            )

    def report_state(self):
        """
        Log the state of the snapshot and raise an exception if the state is
        not ``SUCCESS``
        """
        self.get_state()
        if self.state == 'SUCCESS':
            self.loggit.info('Snapshot {0} successfully completed.'.format(self.name))
        else:
            msg = 'Snapshot {0} completed with state: {0}'.format(self.state)
            self.loggit.error(msg)
            raise exceptions.FailedSnapshot(msg)

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.loggit.info(
            'DRY-RUN: snapshot: {0} in repository {1} with arguments: '
            '{2}'.format(self.name, self.repository, self.body)
        )

    def do_action(self):
        """
        Snapshot indices in `index_list.indices`, with options passed.
        """
        if not self.skip_repo_fs_check:
            utils.test_repo_fs(self.client, self.repository)
        if utils.snapshot_running(self.client):
            raise exceptions.SnapshotInProgress('Snapshot already in progress.')
        try:
            self.loggit.info('Creating snapshot "{0}" from indices: '
                '{1}'.format(self.name, self.index_list.indices)
            )
            # Always set wait_for_completion to False. Let 'utils.wait_for_it' do its
            # thing if wait_for_completion is set to True. Report the task_id
            # either way.
            self.client.snapshot.create(
                repository=self.repository, snapshot=self.name, body=self.body,
                wait_for_completion=False
            )
            if self.wait_for_completion:
                utils.wait_for_it(
                    self.client, 'snapshot', snapshot=self.name,
                    repository=self.repository,
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
                self.report_state()
            else:
                self.loggit.warn(
                    '"wait_for_completion" set to {0}.'
                    'Remember to check for successful completion '
                    'manually.'.format(self.wait_for_completion)
                )
        except Exception as e:
            utils.report_failure(e)

class Restore(object):
    def __init__(self, slo, name=None, indices=None, include_aliases=False,
                ignore_unavailable=False, include_global_state=False,
                partial=False, rename_pattern=None, rename_replacement=None,
                extra_settings={}, wait_for_completion=True, wait_interval=9,
                max_wait=-1, skip_repo_fs_check=False):
        """
        :arg slo: A :class:`curator.snapshotlist.SnapshotList` object
        :arg name: Name of the snapshot to restore.  If no name is provided, it
            will restore the most recent snapshot by age.
        :type name: str
        :arg indices: A list of indices to restore.  If no indices are provided,
            it will restore all indices in the snapshot.
        :type indices: list
        :arg include_aliases: If set to `True`, restore aliases with the
            indices. (default: `False`)
        :type include_aliases: bool
        :arg ignore_unavailable: Ignore unavailable shards/indices.
            (default: `False`)
        :type ignore_unavailable: bool
        :arg include_global_state: Restore cluster global state with snapshot.
            (default: `False`)
        :type include_global_state: bool
        :arg partial: Do not fail if primary shard is unavailable. (default:
            `False`)
        :type partial: bool
        :arg rename_pattern: A regular expression pattern with one or more
            captures, e.g. ``index_(.+)``
        :type rename_pattern: str
        :arg rename_replacement: A target index name pattern with `$#` numbered
            references to the captures in ``rename_pattern``, e.g.
            ``restored_index_$1``
        :type rename_replacement: str
        :arg extra_settings: Extra settings, including shard count and settings
            to omit. For more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html#_changing_index_settings_during_restore
        :type extra_settings: dict, representing the settings.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :type wait_for_completion: bool

        :arg skip_repo_fs_check: Do not validate write access to repository on
            all cluster nodes before proceeding. (default: `False`).  Useful for
            shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success.
        :type skip_repo_fs_check: bool
        """
        self.loggit = logging.getLogger('curator.actions.snapshot')
        utils.verify_snapshot_list(slo)
        # Get the most recent snapshot.
        most_recent = slo.most_recent()
        self.loggit.debug('"most_recent" snapshot: {0}'.format(most_recent))
        #: Instance variable.
        #: Will use a provided snapshot name, or the most recent snapshot in slo
        self.name = name if name else most_recent
        # Stop here now, if it's not a successful snapshot.
        if slo.snapshot_info[self.name]['state'] == 'PARTIAL' \
            and partial == True:
            self.loggit.warn(
                'Performing restore of snapshot in state PARTIAL.')
        elif slo.snapshot_info[self.name]['state'] != 'SUCCESS':
            raise exceptions.CuratorException(
                'Restore operation can only be performed on snapshots with '
                'state "SUCCESS", or "PARTIAL" if partial=True.'
            )
        #: Instance variable.
        #: The Elasticsearch Client object derived from `slo`
        self.client              = slo.client
        #: Instance variable.
        #: Internal reference to `slo`
        self.snapshot_list = slo
        #: Instance variable.
        #: `repository` derived from `slo`
        self.repository          = slo.repository

        if indices:
            self.indices = utils.ensure_list(indices)
        else:
            self.indices = slo.snapshot_info[self.name]['indices']
        self.wfc                 = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait   = max_wait
        #: Instance variable version of ``rename_pattern``
        self.rename_pattern = rename_pattern if rename_replacement is not None \
            else ''
        #: Instance variable version of ``rename_replacement``
        self.rename_replacement = rename_replacement if rename_replacement \
            is not None else ''
        #: Also an instance variable version of ``rename_replacement``
        #: but with Java regex group designations of ``$#``
        #: converted to Python's ``\\#`` style.
        self.py_rename_replacement = self.rename_replacement.replace('$', '\\')
        #: Instance variable.
        #: Internally accessible copy of `skip_repo_fs_check`
        self.skip_repo_fs_check  = skip_repo_fs_check

        #: Instance variable.
        #: Populated at instance creation time from the other options
        self.body                = {
                'indices' : self.indices,
                'include_aliases' : include_aliases,
                'ignore_unavailable' : ignore_unavailable,
                'include_global_state' : include_global_state,
                'partial' : partial,
                'rename_pattern' : self.rename_pattern,
                'rename_replacement' : self.rename_replacement,
            }
        if extra_settings:
            self.loggit.debug(
                'Adding extra_settings to restore body: '
                '{0}'.format(extra_settings)
            )
            try:
                self.body.update(extra_settings)
            except:
                self.loggit.error(
                    'Unable to apply extra settings to restore body')
        self.loggit.debug('REPOSITORY: {0}'.format(self.repository))
        self.loggit.debug('WAIT_FOR_COMPLETION: {0}'.format(self.wfc))
        self.loggit.debug(
            'SKIP_REPO_FS_CHECK: {0}'.format(self.skip_repo_fs_check))
        self.loggit.debug('BODY: {0}'.format(self.body))
        # Populate the expected output index list.
        self._get_expected_output()

    def _get_expected_output(self):
        if not self.rename_pattern and not self.rename_replacement:
            self.expected_output = self.indices
            return # Don't stick around if we're not replacing anything
        self.expected_output = []
        for index in self.indices:
            self.expected_output.append(
                re.sub(
                    self.rename_pattern,
                    self.py_rename_replacement,
                    index
                )
            )
            self.loggit.debug('index: {0} replacement: {1}'.format(index, self.expected_output[-1]))

    def report_state(self):
        """
        Log the state of the restore
        This should only be done if ``wait_for_completion`` is `True`, and only
        after completing the restore.
        """
        all_indices = utils.get_indices(self.client)
        found_count = 0
        missing = []
        for index in self.expected_output:
            if index in all_indices:
                found_count += 1
                self.loggit.info('Found restored index {0}'.format(index))
            else:
                missing.append(index)
        if found_count == len(self.expected_output):
            self.loggit.info('All indices appear to have been restored.')
        else:
            msg = 'Some of the indices do not appear to have been restored. Missing: {0}'.format(missing)
            self.loggit.error(msg)
            raise exceptions.FailedRestore(msg)

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        self.loggit.info(
            'DRY-RUN: restore: Repository: {0} Snapshot name: {1} Arguments: '
            '{2}'.format(
                self.repository, self.name,
                { 'wait_for_completion' : self.wfc, 'body' : self.body }
            )
        )

        for index in self.indices:
            if self.rename_pattern and self.rename_replacement:
                replacement_msg = 'as {0}'.format(
                    re.sub(
                        self.rename_pattern,
                        self.py_rename_replacement,
                        index
                    )
                )
            else:
                replacement_msg = ''
            self.loggit.info(
                'DRY-RUN: restore: Index {0} {1}'.format(index, replacement_msg)
            )

    def do_action(self):
        """
        Restore indices with options passed.
        """
        if not self.skip_repo_fs_check:
            utils.test_repo_fs(self.client, self.repository)
        if utils.snapshot_running(self.client):
            raise exceptions.SnapshotInProgress('Cannot restore while a snapshot is in progress.')
        try:
            self.loggit.info('Restoring indices "{0}" from snapshot: '
                '{1}'.format(self.indices, self.name)
            )
            # Always set wait_for_completion to False. Let 'utils.wait_for_it' do its
            # thing if wait_for_completion is set to True. Report the task_id
            # either way.
            self.client.snapshot.restore(
                repository=self.repository, snapshot=self.name, body=self.body,
                wait_for_completion=False
            )
            if self.wfc:
                utils.wait_for_it(
                    self.client, 'restore', index_list=self.expected_output,
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
                self.report_state()
            else:
                self.loggit.warn(
                    '"wait_for_completion" set to {0}. '
                    'Remember to check for successful completion '
                    'manually.'.format(self.wfc)
                )
        except Exception as e:
            utils.report_failure(e)

class Shrink(object):
    def __init__(self, ilo, shrink_node='DETERMINISTIC', node_filters={},
                number_of_shards=1, number_of_replicas=1,
                shrink_prefix='', shrink_suffix='-shrink',
                copy_aliases=False,
                delete_after=True, post_allocation={},
                wait_for_active_shards=1, wait_for_rebalance=True,
                extra_settings={}, wait_for_completion=True, wait_interval=9,
                max_wait=-1):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg shrink_node: The node name to use as the shrink target, or
            ``DETERMINISTIC``, which will use the values in ``node_filters`` to
            determine which node will be the shrink node.
        :arg node_filters: If the value of ``shrink_node`` is ``DETERMINISTIC``,
            the values in ``node_filters`` will be used while determining which
            node to allocate the shards on before performing the shrink.
        :type node_filters: dict, representing the filters
        :arg number_of_shards: The number of shards the shrunk index should have
        :arg number_of_replicas: The number of replicas for the shrunk index
        :arg shrink_prefix: Prepend the shrunk index with this value
        :arg shrink_suffix: Append the value to the shrunk index (default: `-shrink`)
        :arg copy_aliases: Whether to copy each source index aliases to target index after shrinking.
            the aliases will be added to target index and deleted from source index at the same time(default: `False`)
        :type copy_aliases: bool
        :arg delete_after: Whether to delete each index after shrinking. (default: `True`)
        :type delete_after: bool
        :arg post_allocation: If populated, the `allocation_type`, `key`, and
            `value` will be applied to the shrunk index to re-route it.
        :type post_allocation: dict, with keys `allocation_type`, `key`, and `value`
        :arg wait_for_active_shards: The number of shards expected to be active before returning.
        :arg extra_settings:  Permitted root keys are `settings` and `aliases`.
            See https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html
        :type extra_settings: dict
        :arg wait_for_rebalance: Wait for rebalance. (default: `True`)
        :type wait_for_rebalance: bool
        :arg wait_for_active_shards: Wait for active shards before returning.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  You should not normally change this,
            ever. (default: `True`)
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :type wait_for_completion: bool
        """
        self.loggit = logging.getLogger('curator.actions.shrink')
        utils.verify_index_list(ilo)
        if not 'permit_masters' in node_filters:
            node_filters['permit_masters'] = False
        #: Instance variable. The Elasticsearch Client object derived from `ilo`
        self.client           = ilo.client
        #: Instance variable. Internal reference to `ilo`
        self.index_list       = ilo
        #: Instance variable. Internal reference to `shrink_node`
        self.shrink_node      = shrink_node
        #: Instance variable. Internal reference to `node_filters`
        self.node_filters     = node_filters
        #: Instance variable. Internal reference to `shrink_prefix`
        self.shrink_prefix    = shrink_prefix
        #: Instance variable. Internal reference to `shrink_suffix`
        self.shrink_suffix    = shrink_suffix
        #: Instance variable. Internal reference to `copy_aliases`
        self.copy_aliases = copy_aliases
        #: Instance variable. Internal reference to `delete_after`
        self.delete_after     = delete_after
        #: Instance variable. Internal reference to `post_allocation`
        self.post_allocation  = post_allocation
        #: Instance variable. Internal reference to `wait_for_rebalance`
        self.wait_for_rebalance = wait_for_rebalance
        #: Instance variable. Internal reference to `wait_for_completion`
        self.wfc              = wait_for_completion
        #: Instance variable. How many seconds to wait between checks for completion.
        self.wait_interval    = wait_interval
        #: Instance variable. How long in seconds to `wait_for_completion` before returning with an exception. A value of -1 means wait forever.
        self.max_wait         = max_wait
        #: Instance variable. Internal reference to `number_of_shards`
        self.number_of_shards = number_of_shards
        self.wait_for_active_shards = wait_for_active_shards
        self.shrink_node_name = None
        self.body = {
            'settings': {
                'index.number_of_shards' : number_of_shards,
                'index.number_of_replicas' : number_of_replicas,
            }
        }
        if extra_settings:
            self._merge_extra_settings(extra_settings)

    def _merge_extra_settings(self, extra_settings):
        self.loggit.debug(
            'Adding extra_settings to shrink body: '
            '{0}'.format(extra_settings)
        )
        # Pop these here, otherwise we could overwrite our default number of
        # shards and replicas
        if 'settings' in extra_settings:
            settings = extra_settings.pop('settings')
            try:
                self.body['settings'].update(settings)
            except Exception as e:
                raise exceptions.ConfigurationError('Unable to apply extra settings "{0}" to shrink body. Exception: {1}'.format({'settings':settings}, e))
        if extra_settings:
            try: # Apply any remaining keys, should there be any.
                self.body.update(extra_settings)
            except Exception as e:
                raise exceptions.ConfigurationError('Unable to apply extra settings "{0}" to shrink body. Exception: {1}'.format(extra_settings, e))

    def _data_node(self, node_id):
        roles = utils.node_roles(self.client, node_id)
        name = utils.node_id_to_name(self.client, node_id)
        if not 'data' in roles:
            self.loggit.info('Skipping node "{0}": non-data node'.format(name))
            return False
        if 'master' in roles and not self.node_filters['permit_masters']:
            self.loggit.info('Skipping node "{0}": master node'.format(name))
            return False
        elif 'master' in roles and self.node_filters['permit_masters']:
            self.loggit.warn('Not skipping node "{0}" which is a master node (not recommended), but permit_masters is True'.format(name))
            return True
        else: # It does have `data` as a role.
            return True

    def _exclude_node(self, name):
        if 'exclude_nodes' in self.node_filters:
            if name in self.node_filters['exclude_nodes']:
                self.loggit.info('Excluding node "{0}" due to node_filters'.format(name))
                return True
        return False

    def _shrink_target(self, name):
        return '{0}{1}{2}'.format(self.shrink_prefix, name, self.shrink_suffix)

    def qualify_single_node(self):
        node_id = utils.name_to_node_id(self.client, self.shrink_node)
        if node_id:
            self.shrink_node_id   = node_id
            self.shrink_node_name = self.shrink_node
        else:
            raise exceptions.ConfigurationError('Unable to find node named: "{0}"'.format(self.shrink_node))
        if self._exclude_node(self.shrink_node):
            raise exceptions.ConfigurationError('Node "{0}" listed for exclusion'.format(self.shrink_node))
        if not self._data_node(node_id):
            raise exceptions.ActionError('Node "{0}" is not usable as a shrink node'.format(self.shrink_node))
        self.shrink_node_avail = (
            self.client.nodes.stats()['nodes'][node_id]['fs']['total']['available_in_bytes']
        )

    def most_available_node(self):
        """
        Determine which data node name has the most available free space, and
        meets the other node filters settings.

        :arg client: An :class:`elasticsearch.Elasticsearch` client object
        """
        mvn_avail = 0
        # mvn_total = 0
        mvn_name = None
        mvn_id = None
        nodes = self.client.nodes.stats()['nodes']
        for node_id in nodes:
            name = nodes[node_id]['name']
            if self._exclude_node(name):
                self.loggit.debug('Node "{0}" excluded by node filters'.format(name))
                continue
            if not self._data_node(node_id):
                self.loggit.debug('Node "{0}" is not a data node'.format(name))
                continue
            value = nodes[node_id]['fs']['total']['available_in_bytes']
            if value > mvn_avail:
                mvn_name  = name
                mvn_id    = node_id
                mvn_avail = value
                # mvn_total = nodes[node_id]['fs']['total']['total_in_bytes']
        self.shrink_node_name  = mvn_name
        self.shrink_node_id    = mvn_id
        self.shrink_node_avail = mvn_avail
        # self.shrink_node_total = mvn_total

    def route_index(self, idx, allocation_type, key, value):
        bkey = 'index.routing.allocation.{0}.{1}'.format(allocation_type, key)
        routing = { bkey : value }
        try:
            self.client.indices.put_settings(index=idx, body=routing)
            if self.wait_for_rebalance:
                utils.wait_for_it(self.client, 'allocation', wait_interval=self.wait_interval, max_wait=self.max_wait)
            else:
                utils.wait_for_it(self.client, 'relocate', index=idx, wait_interval=self.wait_interval, max_wait=self.max_wait)
        except Exception as e:
            utils.report_failure(e)

    def __log_action(self, error_msg, dry_run=False):
        if not dry_run:
            raise exceptions.ActionError(error_msg)
        else:
            self.loggit.warn('DRY-RUN: {0}'.format(error_msg))

    def _block_writes(self, idx):
        block = { 'index.blocks.write': True }
        self.client.indices.put_settings(index=idx, body=block)

    def _unblock_writes(self, idx):
        unblock = { 'index.blocks.write': False }
        self.client.indices.put_settings(index=idx, body=unblock)

    def _check_space(self, idx, dry_run=False):
        # Disk watermark calculation is already baked into `available_in_bytes`
        size = utils.index_size(self.client, idx)
        padded = (size * 2) + (32 * 1024)
        if padded < self.shrink_node_avail:
            self.loggit.debug('Sufficient space available for 2x the size of index "{0}".  Required: {1}, available: {2}'.format(idx, padded, self.shrink_node_avail))
        else:
            error_msg = ('Insufficient space available for 2x the size of index "{0}", shrinking will exceed space available. Required: {1}, available: {2}'.format(idx, padded, self.shrink_node_avail))
            self.__log_action(error_msg, dry_run)

    def _check_node(self):
        if self.shrink_node != 'DETERMINISTIC':
            if not self.shrink_node_name:
                self.qualify_single_node()
        else:
            self.most_available_node()
        # At this point, we should have the three shrink-node identifying
        # instance variables:
        # - self.shrink_node_name
        # - self.shrink_node_id
        # - self.shrink_node_avail
        # # - self.shrink_node_total - only if needed in the future

    def _check_target_exists(self, idx, dry_run=False):
        target = self._shrink_target(idx)
        if self.client.indices.exists(target):
            error_msg = 'Target index "{0}" already exists'.format(target)
            self.__log_action(error_msg, dry_run)

    def _check_doc_count(self, idx, dry_run=False):
        max_docs = 2147483519
        doc_count = self.client.indices.stats(idx)['indices'][idx]['primaries']['docs']['count']
        if doc_count > (max_docs * self.number_of_shards):
            error_msg = ('Too many documents ({0}) to fit in {1} shard(s). Maximum number of docs per shard is {2}'.format(doc_count, self.number_of_shards, max_docs))
            self.__log_action(error_msg, dry_run)

    def _check_shard_count(self, idx, src_shards, dry_run=False):
        if self.number_of_shards >= src_shards:
            error_msg = ('Target number of shards ({0}) must be less than current number of shards ({1}) in index "{2}"'.format(self.number_of_shards, src_shards, idx))
            self.__log_action(error_msg, dry_run)

    def _check_shard_factor(self, idx, src_shards, dry_run=False):
        # Find the list of factors of src_shards
        factors = [x for x in range(1,src_shards+1) if src_shards % x == 0]
        # Pop the last one, because it will be the value of src_shards
        factors.pop()
        if not self.number_of_shards in factors:
            error_msg = (
                '"{0}" is not a valid factor of {1} shards.  Valid values are '
                '{2}'.format(self.number_of_shards, src_shards, factors)
            )
            self.__log_action(error_msg, dry_run)

    def _check_all_shards(self, idx):
        shards = self.client.cluster.state(index=idx)['routing_table']['indices'][idx]['shards']
        found = []
        for shardnum in shards:
            for shard_idx in range(0, len(shards[shardnum])):
                if shards[shardnum][shard_idx]['node'] == self.shrink_node_id:
                    found.append({'shard': shardnum, 'primary': shards[shardnum][shard_idx]['primary']})
        if len(shards) != len(found):
            self.loggit.debug('Found these shards on node "{0}": {1}'.format(self.shrink_node_name, found))
            raise exceptions.ActionError('Unable to shrink index "{0}" as not all shards were found on the designated shrink node ({1}): {2}'.format(idx, self.shrink_node_name, found))

    def pre_shrink_check(self, idx, dry_run=False):
        self.loggit.debug('BEGIN PRE_SHRINK_CHECK')
        self.loggit.debug('Check that target exists')
        self._check_target_exists(idx, dry_run)
        self.loggit.debug('Check doc count constraints')
        self._check_doc_count(idx, dry_run)
        self.loggit.debug('Check shard count')
        src_shards = int(self.client.indices.get(idx)[idx]['settings']['index']['number_of_shards'])
        self._check_shard_count(idx, src_shards, dry_run)
        self.loggit.debug('Check shard factor')
        self._check_shard_factor(idx, src_shards, dry_run)
        self.loggit.debug('Check node availability')
        self._check_node()
        self.loggit.debug('Check available disk space')
        self._check_space(idx, dry_run)
        self.loggit.debug('FINISH PRE_SHRINK_CHECK')

    def do_copy_aliases(self, source_idx, target_idx):
        alias_actions = []
        aliases = self.client.indices.get_alias(index=source_idx)
        for alias in aliases[source_idx]['aliases']:
            self.loggit.debug('alias: {0}'.format(alias))
            alias_actions.append(
                {'remove': {'index': source_idx, 'alias': alias}})
            alias_actions.append(
                {'add': {'index': target_idx, 'alias': alias}})
        if alias_actions:
            self.loggit.info('Copy alias actions: {0}'.format(alias_actions))
            self.client.indices.update_aliases({ 'actions' : alias_actions })

    def do_dry_run(self):
        """
        Show what a regular run would do, but don't actually do it.
        """
        self.index_list.filter_closed()
        self.index_list.filter_by_shards(number_of_shards=self.number_of_shards)
        self.index_list.empty_list_check()
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                for idx in l: # Shrink can only be done one at a time...
                    target = self._shrink_target(idx)
                    self.pre_shrink_check(idx, dry_run=True)
                    self.loggit.info('DRY-RUN: Moving shards to shrink node: "{0}"'.format(self.shrink_node_name))
                    self.loggit.info('DRY-RUN: Shrinking index "{0}" to "{1}" with settings: {2}, wait_for_active_shards={3}'.format(idx, target, self.body, self.wait_for_active_shards))
                    if self.post_allocation:
                        self.loggit.info('DRY-RUN: Applying post-shrink allocation rule "{0}" to index "{1}"'.format('index.routing.allocation.{0}.{1}:{2}'.format(self.post_allocation['allocation_type'], self.post_allocation['key'], self.post_allocation['value']), target))
                    if self.copy_aliases:
                        self.loggit.info('DRY-RUN: Copy source index aliases "{0}"'.format(self.client.indices.get_alias(idx)))
                        #self.do_copy_aliases(idx, target)
                    if self.delete_after:
                        self.loggit.info('DRY-RUN: Deleting source index "{0}"'.format(idx))
        except Exception as e:
            utils.report_failure(e)

    def do_action(self):
        self.index_list.filter_closed()
        self.index_list.filter_by_shards(number_of_shards=self.number_of_shards)
        self.index_list.empty_list_check()
        self.loggit.info('Shrinking {0} selected indices: {1}'.format(len(self.index_list.indices), self.index_list.indices))
        try:
            index_lists = utils.chunk_index_list(self.index_list.indices)
            for l in index_lists:
                for idx in l: # Shrink can only be done one at a time...
                    target = self._shrink_target(idx)
                    self.loggit.info('Source index: {0} -- Target index: {1}'.format(idx, target))
                    # Pre-check ensures disk space available for each pass of the loop
                    self.pre_shrink_check(idx)
                    # Route the index to the shrink node
                    self.loggit.info('Moving shards to shrink node: "{0}"'.format(self.shrink_node_name))
                    self.route_index(idx, 'require', '_name', self.shrink_node_name)
                    # Ensure a copy of each shard is present
                    self._check_all_shards(idx)
                    # Block writes on index
                    self._block_writes(idx)
                    # Do final health check
                    if not utils.health_check(self.client, status='green'):
                        raise exceptions.ActionError('Unable to proceed with shrink action. Cluster health is not "green"')
                    # Do the shrink
                    self.loggit.info('Shrinking index "{0}" to "{1}" with settings: {2}, wait_for_active_shards={3}'.format(idx, target, self.body, self.wait_for_active_shards))
                    try:
                        self.client.indices.shrink(index=idx, target=target, body=self.body, wait_for_active_shards=self.wait_for_active_shards)
                        # Wait for it to complete
                        if self.wfc:
                            self.loggit.debug('Wait for shards to complete allocation for index: {0}'.format(target))
                            if self.wait_for_rebalance:
                                utils.wait_for_it(self.client, 'shrink', wait_interval=self.wait_interval, max_wait=self.max_wait)
                            else:
                                utils.wait_for_it(self.client, 'relocate', index=target, wait_interval=self.wait_interval, max_wait=self.max_wait)
                    except Exception as e:
                        if self.client.indices.exists(index=target):
                            self.loggit.error('Deleting target index "{0}" due to failure to complete shrink'.format(target))
                            self.client.indices.delete(index=target)
                        raise exceptions.ActionError('Unable to shrink index "{0}" -- Error: {1}'.format(idx, e))
                    self.loggit.info('Index "{0}" successfully shrunk to "{1}"'.format(idx, target))
                    # Do post-shrink steps
                    # Unblock writes on index (just in case)
                    self._unblock_writes(idx)
                    ## Post-allocation, if enabled
                    if self.post_allocation:
                        self.loggit.info('Applying post-shrink allocation rule "{0}" to index "{1}"'.format('index.routing.allocation.{0}.{1}:{2}'.format(self.post_allocation['allocation_type'], self.post_allocation['key'], self.post_allocation['value']), target))
                        self.route_index(target, self.post_allocation['allocation_type'], self.post_allocation['key'], self.post_allocation['value'])
                    ## Copy aliases, if flagged
                    if self.copy_aliases:
                        self.loggit.info('Copy source index aliases "{0}"'.format(idx))
                        self.do_copy_aliases(idx, target)
                    ## Delete, if flagged
                    if self.delete_after:
                        self.loggit.info('Deleting source index "{0}"'.format(idx))
                        self.client.indices.delete(index=idx)
                    else: # Let's unset the routing we applied here.
                        self.loggit.info('Unassigning routing for source index: "{0}"'.format(idx))
                        self.route_index(idx, 'require', '_name', '')

        except Exception as e:
            # Just in case it fails after attempting to meet this condition
            self._unblock_writes(idx)
            utils.report_failure(e)

