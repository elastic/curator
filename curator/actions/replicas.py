"""Index replica count action class"""
import logging
from curator.exceptions import MissingArgument
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import chunk_index_list, report_failure, show_dry_run, to_csv
from curator.helpers.waiters import wait_for_it

class Replicas:
    """Replica Action Class"""
    def __init__(self, ilo, count=None, wait_for_completion=False, wait_interval=9, max_wait=-1):
        """
        :param ilo: An IndexList Object
        :param count: The count of replicas per shard
        :param wait_for_completion: Wait for completion before returning.
        :param wait_interval: Seconds to wait between completion checks.
        :param max_wait: Maximum number of seconds to ``wait_for_completion``

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type count: int
        :type wait_for_completion: bool
        :type wait_interval: int
        :type max_wait: int
        """
        verify_index_list(ilo)
        # It's okay for count to be zero
        if count == 0:
            pass
        elif not count:
            raise MissingArgument('Missing value for "count"')

        #: The :py:class:`~.curator.indexlist.IndexList` object passed from param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        #: Object attribute that gets the value of param ``count``.
        self.count = count
        #: Object attribute that gets the value of param ``wait_for_completion``.
        self.wfc = wait_for_completion
        #: Object attribute that gets the value of param ``wait_interval``.
        self.wait_interval = wait_interval
        #: Object attribute that gets the value of param ``max_wait``.
        self.max_wait = max_wait
        self.loggit = logging.getLogger('curator.actions.replicas')

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        show_dry_run(self.index_list, 'replicas', count=self.count)

    def do_action(self):
        """
        Update ``number_of_replicas`` with :py:attr:`count` and
        :py:meth:`~.elasticsearch.client.IndicesClient.put_settings` to indices in
        :py:attr:`index_list`
        """
        self.loggit.debug(
            'Cannot get update replica count of closed indices. Omitting any closed indices.')
        self.index_list.filter_closed()
        self.index_list.empty_list_check()
        msg = (
            f'Setting the replica count to {self.count} for {len(self.index_list.indices)} '
            f'indices: {self.index_list.indices}'
        )
        self.loggit.info(msg)
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                self.client.indices.put_settings(
                    index=to_csv(lst), settings={'number_of_replicas': self.count}
                )
                if self.wfc and self.count > 0:
                    msg = (
                        f'Waiting for shards to complete replication for indices: {to_csv(lst)}')
                    self.loggit.debug(msg)
                    wait_for_it(
                        self.client, 'replicas',
                        wait_interval=self.wait_interval, max_wait=self.max_wait
                    )
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
