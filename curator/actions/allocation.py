"""Allocation action class"""
import logging
# pylint: disable=import-error
from curator.exceptions import MissingArgument
from curator.helpers.testers import verify_index_list
from curator.helpers.waiters import wait_for_it
from curator.helpers.utils import chunk_index_list, report_failure, show_dry_run, to_csv

class Allocation:
    """Allocation Action Class"""
    def __init__(
            self, ilo, key=None, value=None, allocation_type='require', wait_for_completion=False,
            wait_interval=3, max_wait=-1
        ):
        """
        :param ilo: An IndexList Object
        :param key: An arbitrary metadata attribute key.  Must match the key assigned to at least
        :param value: An arbitrary metadata attribute value.  Must correspond to values associated
        :param allocation_type: Type of allocation to apply. Default is ``require``
        :param wait_for_completion: Wait for completion before returning.
        :param wait_interval: Seconds to wait between completion checks.
        :param max_wait: Maximum number of seconds to ``wait_for_completion``

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type key: str
        :type value: str
        :type allocation_type: str
        :type wait_for_completion: bool
        :type wait_interval: int
        :type max_wait: int

        .. note::
            See more about `shard allocation filtering </https://www.elastic.co/guide/en/elasticsearch/reference/8.6/shard-allocation-filtering.html>`_.
        """
        verify_index_list(ilo)
        if not key:
            raise MissingArgument('No value for "key" provided')
        if allocation_type not in ['require', 'include', 'exclude']:
            raise ValueError(
                f'{allocation_type} is an invalid allocation_type.  Must be one of "require", '
                '"include", "exclude".'
            )
        #: The :py:class:`~.curator.indexlist.IndexList` object passed as ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        self.loggit = logging.getLogger('curator.actions.allocation')
        bkey = f'index.routing.allocation.{allocation_type}.{key}'
        #: Populated at instance creation time. Value is built from the passed params
        #: ``allocation_type``, ``key``, and ``value``, e.g.
        #: ``index.routing.allocation.allocation_type.key.value``
        self.settings = {bkey : value}
        #: Object attribute that gets the value of param ``wait_for_completion``
        self.wfc = wait_for_completion
        #: Object attribute that gets the value of param ``wait_interval``
        self.wait_interval = wait_interval
        #: Object attribute that gets the value of param ``max_wait``
        self.max_wait = max_wait

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        show_dry_run(self.index_list, 'allocation', settings=self.settings)

    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.IndicesClient.put_settings` to indices in
        :py:attr:`index_list` with :py:attr:`settings`.
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
                        'Waiting for shards to complete relocation for indices: %s', to_csv(lst))
                    wait_for_it(
                        self.client, 'allocation',
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
