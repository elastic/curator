"""Reindex action class"""

import logging

# pylint: disable=broad-except
from curator.defaults.settings import DATA_NODE_ROLES
from curator.exceptions import ActionError, ConfigurationError
from curator.helpers.getters import (
    index_size,
    name_to_node_id,
    node_id_to_name,
    node_roles,
)
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import chunk_index_list, report_failure
from curator.helpers.waiters import health_check, wait_for_it


class Shrink:
    """Shrink Action Class"""

    def __init__(
        self,
        ilo,
        shrink_node='DETERMINISTIC',
        node_filters=None,
        number_of_shards=1,
        number_of_replicas=1,
        shrink_prefix='',
        shrink_suffix='-shrink',
        copy_aliases=False,
        delete_after=True,
        post_allocation=None,
        wait_for_active_shards=1,
        wait_for_rebalance=True,
        extra_settings=None,
        wait_for_completion=True,
        wait_interval=9,
        max_wait=-1,
    ):
        """
        :param ilo: An IndexList Object
        :param shrink_node: The node name to use as the shrink target, or
            ``DETERMINISTIC``, which will use the values in ``node_filters`` to
            determine which node will be the shrink node.
        :param node_filters: If the value of ``shrink_node`` is ``DETERMINISTIC``,
            the values in ``node_filters`` will be used while determining which
            node to allocate the shards on before performing the shrink.
        :param number_of_shards: The number of shards the shrunk index should have
        :param number_of_replicas: The number of replicas for the shrunk index
        :param shrink_prefix: Prepend the shrunk index with this value
        :param shrink_suffix: Append the value to the shrunk index
            (Default: ``-shrink``)
        :param copy_aliases: Whether to copy each source index aliases to target
            index after shrinking. The aliases will be added to target index and
            deleted from source index at the same time. (Default: ``False``)
        :param delete_after: Whether to delete each index after shrinking.
            (Default: ``True``)
        :param post_allocation: If populated, the ``allocation_type``, ``key``,
            and ``value`` will be applied to the shrunk index to re-route it.
        :param extra_settings:  Permitted root keys are ``settings`` and ``aliases``.
        :param wait_for_active_shards: Wait for this many active shards before
            returning.
        :param wait_for_rebalance: Wait for rebalance. (Default: ``True``)
        :param wait_for_completion: Wait for completion before returning.
        :param wait_interval: Seconds to wait between completion checks.
        :param max_wait: Maximum number of seconds to ``wait_for_completion``

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type shrink_node: str
        :type node_filters: dict
        :type number_of_shards: int
        :type number_of_replicas: int
        :type shrink_prefix: str
        :type shrink_suffix: str
        :type copy_aliases: bool
        :type delete_after: bool
        :type post_allocation: dict
        :type extra_settings: dict
        :type wait_for_active_shards: int
        :type wait_for_rebalance: bool
        :type wait_for_completion: bool
        :type wait_interval: int
        :type max_wait: int
        """
        if node_filters is None:
            node_filters = {}
        if post_allocation is None:
            post_allocation = {}
        if extra_settings is None:
            extra_settings = {}
        self.loggit = logging.getLogger('curator.actions.shrink')
        verify_index_list(ilo)
        if 'permit_masters' not in node_filters:
            node_filters['permit_masters'] = False
        #: The :py:class:`~.curator.indexlist.IndexList` object passed from
        #: param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        #: Object attribute that gets the value of param ``shrink_node``.
        self.shrink_node = shrink_node
        #: Object attribute that gets the value of param ``node_filters``.
        self.node_filters = node_filters
        #: Object attribute that gets the value of param ``shrink_prefix``.
        self.shrink_prefix = shrink_prefix
        #: Object attribute that gets the value of param ``shrink_suffix``.
        self.shrink_suffix = shrink_suffix
        #: Object attribute that gets the value of param ``copy_aliases``.
        self.copy_aliases = copy_aliases
        #: Object attribute that gets the value of param ``delete_after``.
        self.delete_after = delete_after
        #: Object attribute that gets the value of param ``post_allocation``.
        self.post_allocation = post_allocation
        #: Object attribute that gets the value of param ``wait_for_rebalance``.
        self.wait_for_rebalance = wait_for_rebalance
        #: Object attribute that gets the value of param ``wait_for_completion``.
        self.wfc = wait_for_completion
        #: Object attribute that gets the value of param ``wait_interval``.
        self.wait_interval = wait_interval
        #: Object attribute that gets the value of param ``max_wait``.
        self.max_wait = max_wait
        #: Object attribute that gets the value of param ``number_of_shards``.
        self.number_of_shards = number_of_shards
        #: Object attribute that gets the value of param ``wait_for_active_shards``.
        self.wait_for_active_shards = wait_for_active_shards

        #: Object attribute that represents the target node for shrinking.
        self.shrink_node_name = None
        #: Object attribute that represents whether :py:attr:`shrink_node_name`
        #: is available
        self.shrink_node_avail = None
        #: Object attribute that represents the node_id of :py:attr:`shrink_node_name`
        self.shrink_node_id = None

        #: Object attribute that gets values from params ``number_of_shards`` and
        #: ``number_of_replicas``.
        self.settings = {
            'index.number_of_shards': number_of_shards,
            'index.number_of_replicas': number_of_replicas,
        }

        if extra_settings:
            self._merge_extra_settings(extra_settings)

        self._merge_extra_settings(
            {
                'settings': {
                    'index.routing.allocation.require._name': None,
                    'index.blocks.write': None,
                }
            }
        )

    def _merge_extra_settings(self, extra_settings):
        self.loggit.debug('Adding extra_settings to shrink body: %s', extra_settings)
        # Pop these here, otherwise we could overwrite our default number of
        # shards and replicas
        if 'settings' in extra_settings:
            settings = extra_settings.pop('settings')
            try:
                self.settings.update(settings)
            except Exception as exc:
                raise ConfigurationError(
                    f"Unable to apply extra settings \"{{'settings':settings}}\" "
                    f"to shrink body. Exception: {exc}"
                ) from exc
        if extra_settings:
            try:  # Apply any remaining keys, should there be any.
                self.settings.update(extra_settings)
            except Exception as exc:
                raise ConfigurationError(
                    f'Unable to apply extra settings "{extra_settings}" '
                    f'to shrink body. Exception: {exc}'
                ) from exc

    def _data_node(self, node_id):
        roles = node_roles(self.client, node_id)
        name = node_id_to_name(self.client, node_id)
        is_data_node = False
        for role in roles:
            if role in DATA_NODE_ROLES:
                is_data_node = True
                break  # At least one data node role type qualifies
        if not is_data_node:
            self.loggit.info('Skipping node "%s": non-data node', name)
            return False
        if 'master' in roles and not self.node_filters['permit_masters']:
            self.loggit.info('Skipping node "%s": master node', name)
            return False
        if 'master' in roles and self.node_filters['permit_masters']:
            msg = (
                f'Not skipping node "{name}" which is a master node (not recommended)'
                f', but permit_masters is True'
            )
            self.loggit.warning(msg)
            return True
        # Implied else: It does have a qualifying data role and is not a master node
        return True

    def _exclude_node(self, name):
        if 'exclude_nodes' in self.node_filters:
            if name in self.node_filters['exclude_nodes']:
                self.loggit.info('Excluding node "%s" due to node_filters', name)
                return True
        return False

    def _shrink_target(self, name):
        return f'{self.shrink_prefix}{name}{self.shrink_suffix}'

    def qualify_single_node(self):
        """Qualify a single node as a shrink target"""
        node_id = name_to_node_id(self.client, self.shrink_node)
        if node_id:
            self.shrink_node_id = node_id
            self.shrink_node_name = self.shrink_node
        else:
            raise ConfigurationError(f'Unable to find node named: "{self.shrink_node}"')
        if self._exclude_node(self.shrink_node):
            raise ConfigurationError(f'Node "{self.shrink_node}" listed for exclusion')
        if not self._data_node(node_id):
            raise ActionError(
                f'Node "{self.shrink_node}" is not usable as a shrink node'
            )
        self.shrink_node_avail = self.client.nodes.stats()['nodes'][node_id]['fs'][
            'total'
        ]['available_in_bytes']

    def most_available_node(self):
        """
        Determine which data node name has the most available free space, and meets
        the other node filters settings.
        """
        mvn_avail = 0
        # mvn_total = 0
        mvn_name = None
        mvn_id = None
        nodes = self.client.nodes.stats()['nodes']
        for node_id in nodes:
            name = nodes[node_id]['name']
            if self._exclude_node(name):
                self.loggit.debug('Node "%s" excluded by node filters', name)
                continue
            if not self._data_node(node_id):
                self.loggit.debug('Node "%s" is not a data node', name)
                continue
            value = nodes[node_id]['fs']['total']['available_in_bytes']
            if value > mvn_avail:
                mvn_name = name
                mvn_id = node_id
                mvn_avail = value
        self.shrink_node_name = mvn_name
        self.shrink_node_id = mvn_id
        self.shrink_node_avail = mvn_avail

    def route_index(self, idx, allocation_type, key, value):
        """Apply the indicated shard routing allocation"""
        bkey = f'index.routing.allocation.{allocation_type}.{key}'
        routing = {bkey: value}
        try:
            self.client.indices.put_settings(index=idx, settings=routing)
            if self.wait_for_rebalance:
                wait_for_it(
                    self.client,
                    'allocation',
                    wait_interval=self.wait_interval,
                    max_wait=self.max_wait,
                )
            else:
                wait_for_it(
                    self.client,
                    'relocate',
                    index=idx,
                    wait_interval=self.wait_interval,
                    max_wait=self.max_wait,
                )
        except Exception as err:
            report_failure(err)

    def __log_action(self, error_msg, dry_run=False):
        if not dry_run:
            raise ActionError(error_msg)
        else:
            self.loggit.warning('DRY-RUN: %s', error_msg)

    def _block_writes(self, idx):
        block = {'index.blocks.write': True}
        self.client.indices.put_settings(index=idx, settings=block)

    def _unblock_writes(self, idx):
        unblock = {'index.blocks.write': False}
        self.client.indices.put_settings(index=idx, settings=unblock)

    def _check_space(self, idx, dry_run=False):
        # Disk watermark calculation is already baked into `available_in_bytes`
        size = index_size(self.client, idx, value='primaries')
        padded = (size * 2) + (32 * 1024)
        if padded < self.shrink_node_avail:
            msg = (
                f'Sufficient space available for 2x the size of index "{idx}". '
                f'Required: {padded}, available: {self.shrink_node_avail}'
            )
            self.loggit.debug(msg)
        else:
            error_msg = (
                f'Insufficient space available for 2x the size of index "{idx}", '
                f'shrinking will exceed space available. Required: {padded}, '
                f'available: {self.shrink_node_avail}'
            )
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
        if self.client.indices.exists(index=target):
            error_msg = f'Target index "{target}" already exists'
            self.__log_action(error_msg, dry_run)

    def _check_doc_count(self, idx, dry_run=False):
        max_docs = 2147483519
        doc_count = self.client.indices.stats(index=idx)['indices'][idx]['primaries'][
            'docs'
        ]['count']
        if doc_count > (max_docs * self.number_of_shards):
            error_msg = (
                f'Too many documents ({doc_count}) to fit in {self.number_of_shards} '
                f'shard(s). Maximum number of docs per shard is {max_docs}'
            )
            self.__log_action(error_msg, dry_run)

    def _check_shard_count(self, idx, src_shards, dry_run=False):
        if self.number_of_shards >= src_shards:
            error_msg = (
                f'Target number of shards ({self.number_of_shards}) must be less than '
                f'current number of shards ({src_shards}) in index "{idx}"'
            )
            self.__log_action(error_msg, dry_run)

    def _check_shard_factor(self, idx, src_shards, dry_run=False):
        # Find the list of factors of src_shards
        factors = [x for x in range(1, src_shards + 1) if src_shards % x == 0]
        # Pop the last one, because it will be the value of src_shards
        factors.pop()
        if self.number_of_shards not in factors:
            error_msg = (
                f'"{self.number_of_shards}" is not a valid factor of {src_shards} '
                f'shards of index {idx}. Valid values are {factors}'
            )
            self.__log_action(error_msg, dry_run)

    def _check_all_shards(self, idx):
        shards = self.client.cluster.state(index=idx)['routing_table']['indices'][idx][
            'shards'
        ]
        found = []
        for shardnum in shards:
            for shard_idx in range(0, len(shards[shardnum])):
                if shards[shardnum][shard_idx]['node'] == self.shrink_node_id:
                    found.append(
                        {
                            'shard': shardnum,
                            'primary': shards[shardnum][shard_idx]['primary'],
                        }
                    )
        if len(shards) != len(found):
            self.loggit.debug(
                'Found these shards on node "%s": %s', self.shrink_node_name, found
            )
            raise ActionError(
                f'Unable to shrink index "{idx}" as not all shards were found on the '
                f'designated shrink node ({self.shrink_node_name}): {found}'
            )

    def pre_shrink_check(self, idx, dry_run=False):
        """Do a shrink preflight check"""
        self.loggit.debug('BEGIN PRE_SHRINK_CHECK')
        self.loggit.debug('Check that target exists')
        self._check_target_exists(idx, dry_run)
        self.loggit.debug('Check doc count constraints')
        self._check_doc_count(idx, dry_run)
        self.loggit.debug('Check shard count')
        src_shards = int(
            self.client.indices.get(index=idx)[idx]['settings']['index'][
                'number_of_shards'
            ]
        )
        self._check_shard_count(idx, src_shards, dry_run)
        self.loggit.debug('Check shard factor')
        self._check_shard_factor(idx, src_shards, dry_run)
        self.loggit.debug('Check node availability')
        self._check_node()
        self.loggit.debug('Check available disk space')
        self._check_space(idx, dry_run)
        self.loggit.debug('FINISH PRE_SHRINK_CHECK')

    def do_copy_aliases(self, source_idx, target_idx):
        """Copy the aliases to the shrunk index"""
        alias_actions = []
        aliases = self.client.indices.get_alias(index=source_idx)
        for alias in aliases[source_idx]['aliases']:
            self.loggit.debug('alias: %s', alias)
            alias_actions.append({'remove': {'index': source_idx, 'alias': alias}})
            alias_actions.append({'add': {'index': target_idx, 'alias': alias}})
        if alias_actions:
            self.loggit.info('Copy alias actions: %s', alias_actions)
            self.client.indices.update_aliases(actions=alias_actions)

    def do_dry_run(self):
        """Show what a regular run would do, but don't actually do it."""
        self.index_list.filter_closed()
        self.index_list.filter_by_shards(number_of_shards=self.number_of_shards)
        self.index_list.empty_list_check()
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                for idx in lst:  # Shrink can only be done one at a time...
                    target = self._shrink_target(idx)
                    self.pre_shrink_check(idx, dry_run=True)
                    self.loggit.info(
                        'DRY-RUN: Moving shards to shrink node: "%s"',
                        self.shrink_node_name,
                    )
                    msg = (
                        f'DRY-RUN: Shrinking index "{idx}" to "{target}" with '
                        f'settings: {self.settings}, wait_for_active_shards='
                        f'{self.wait_for_active_shards}'
                    )
                    self.loggit.info(msg)
                    if self.post_allocation:
                        submsg = (
                            f"index.routing.allocation."
                            f"{self.post_allocation['allocation_type']}."
                            f"{self.post_allocation['key']}:"
                            f"{self.post_allocation['value']}"
                        )
                        msg = (
                            f'DRY-RUN: Applying post-shrink allocation rule "{submsg}" '
                            f'to index "{target}"'
                        )
                        self.loggit.info(msg)
                    if self.copy_aliases:
                        msg = (
                            f'DRY-RUN: Copy source index aliases '
                            f'"{self.client.indices.get_alias(index=idx)}"'
                        )
                        self.loggit.info(msg)
                    if self.delete_after:
                        self.loggit.info('DRY-RUN: Deleting source index "%s"', idx)
        except Exception as err:
            report_failure(err)

    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.IndicesClient.shrink` the indices in
        :py:attr:`index_list`
        """
        self.index_list.filter_closed()
        self.index_list.filter_by_shards(number_of_shards=self.number_of_shards)
        self.index_list.empty_list_check()
        msg = (
            f'Shrinking {len(self.index_list.indices)} selected indices: '
            f'{self.index_list.indices}'
        )
        self.loggit.info(msg)
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                for idx in lst:  # Shrink can only be done one at a time...
                    target = self._shrink_target(idx)
                    self.loggit.info(
                        'Source index: %s -- Target index: %s', idx, target
                    )
                    # Pre-check ensures disk space available for each pass of the loop
                    self.pre_shrink_check(idx)
                    # Route the index to the shrink node
                    self.loggit.info(
                        'Moving shards to shrink node: "%s"', self.shrink_node_name
                    )
                    self.route_index(idx, 'require', '_name', self.shrink_node_name)
                    # Ensure a copy of each shard is present
                    self._check_all_shards(idx)
                    # Block writes on index
                    self._block_writes(idx)
                    # Do final health check
                    if not health_check(self.client, status='green'):
                        msg = (
                            'Unable to proceed with shrink action. '
                            'Cluster health is not "green"'
                        )
                        raise ActionError(msg)
                    # Do the shrink
                    msg = (
                        f'Shrinking index "{idx}" to "{target}" with settings: '
                        f'{self.settings}, wait_for_active_shards='
                        f'{self.wait_for_active_shards}'
                    )
                    self.loggit.info(msg)
                    try:
                        self.client.indices.shrink(
                            index=idx,
                            target=target,
                            settings=self.settings,
                            wait_for_active_shards=self.wait_for_active_shards,
                        )
                        # Wait for it to complete
                        if self.wfc:
                            self.loggit.debug(
                                'Wait for shards to complete allocation for index: %s',
                                target,
                            )
                            if self.wait_for_rebalance:
                                wait_for_it(
                                    self.client,
                                    'shrink',
                                    wait_interval=self.wait_interval,
                                    max_wait=self.max_wait,
                                )
                            else:
                                wait_for_it(
                                    self.client,
                                    'relocate',
                                    index=target,
                                    wait_interval=self.wait_interval,
                                    max_wait=self.max_wait,
                                )
                    except Exception as exc:
                        if self.client.indices.exists(index=target):
                            msg = (
                                f'Deleting target index "{target}" due to failure '
                                f'to complete shrink'
                            )
                            self.loggit.error(msg)
                            self.client.indices.delete(index=target)
                        raise ActionError(
                            f'Unable to shrink index "{idx}" -- Error: {exc}'
                        ) from exc
                    self.loggit.info(
                        'Index "%s" successfully shrunk to "%s"', idx, target
                    )
                    # Do post-shrink steps
                    # Unblock writes on index (just in case)
                    self._unblock_writes(idx)
                    # Post-allocation, if enabled
                    if self.post_allocation:
                        submsg = (
                            f"index.routing.allocation."
                            f"{self.post_allocation['allocation_type']}."
                            f"{self.post_allocation['key']}:"
                            f"{self.post_allocation['value']}"
                        )
                        msg = (
                            f'Applying post-shrink allocation rule "{submsg}" '
                            f'to index "{target}"'
                        )
                        self.loggit.info(msg)
                        self.route_index(
                            target,
                            self.post_allocation['allocation_type'],
                            self.post_allocation['key'],
                            self.post_allocation['value'],
                        )
                    # Copy aliases, if flagged
                    if self.copy_aliases:
                        self.loggit.info('Copy source index aliases "%s"', idx)
                        self.do_copy_aliases(idx, target)
                    # Delete, if flagged
                    if self.delete_after:
                        self.loggit.info('Deleting source index "%s"', idx)
                        self.client.indices.delete(index=idx)
                    else:  # Let's unset the routing we applied here.
                        self.loggit.info(
                            'Unassigning routing for source index: "%s"', idx
                        )
                        self.route_index(idx, 'require', '_name', '')

        except Exception as err:
            # Just in case it fails after attempting to meet this condition
            self._unblock_writes(idx)
            report_failure(err)
