"""Allocation action class"""
import logging
# pylint: disable=import-error
from curator.exceptions import MissingArgument
from curator.utils import (
    chunk_index_list, report_failure, show_dry_run, to_csv, wait_for_it, verify_index_list)

class Allocation:
    """Allocation Action Class"""
    def __init__(
            self, ilo, key=None, value=None, allocation_type='require', wait_for_completion=False,
            wait_interval=3, max_wait=-1
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
            https://www.elastic.co/guide/en/elasticsearch/reference/6.8/shard-allocation-filtering.html
        """
        verify_index_list(ilo)
        if not key:
            raise MissingArgument('No value for "key" provided')
        if allocation_type not in ['require', 'include', 'exclude']:
            raise ValueError(
                f'{allocation_type} is an invalid allocation_type.  Must be one of "require", '
                '"include", "exclude".'
            )
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        self.loggit = logging.getLogger('curator.actions.allocation')
        #: Instance variable.
        #: Populated at instance creation time. Value is
        #: ``index.routing.allocation.`` `allocation_type` ``.`` `key` ``.`` `value`
        bkey = f'index.routing.allocation.{allocation_type}.{key}'
        self.settings = {bkey : value}
        #: Instance variable.
        #: Internal reference to `wait_for_completion`
        self.wfc = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait = max_wait

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        show_dry_run(self.index_list, 'allocation', settings=self.settings)

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
        self.loggit.info(
            'Updating %s selected indices: %s', len(self.index_list.indices),
            self.index_list.indices
        )
        self.loggit.info('Updating index setting %s', self.settings)
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                self.client.indices.put_settings(
                    index=to_csv(lst), settings=self.settings
                )
                if self.wfc:
                    self.loggit.debug(
                        'Waiting for shards to complete relocation for indices:'
                        ' %s', to_csv(lst)
                    )
                    wait_for_it(
                        self.client, 'allocation',
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
