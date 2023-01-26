"""Index replica count action class"""
import logging
# pylint: disable=import-error
from curator.exceptions import MissingArgument
from curator.utils import (
    chunk_index_list, report_failure, show_dry_run, to_csv, verify_index_list, wait_for_it)

class Replicas:
    """Replica Action Class"""
    def __init__(
            self, ilo, count=None, wait_for_completion=False, wait_interval=9, max_wait=-1):
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
        verify_index_list(ilo)
        # It's okay for count to be zero
        if count == 0:
            pass
        elif not count:
            raise MissingArgument('Missing value for "count"')
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `count`
        self.count = count
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
        self.loggit = logging.getLogger('curator.actions.replicas')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        show_dry_run(self.index_list, 'replicas', count=self.count)

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
