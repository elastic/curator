"""Forcemerge action class"""
import logging
from time import sleep
# pylint: disable=import-error
from curator.exceptions import MissingArgument
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import report_failure, show_dry_run

class ForceMerge:
    """ForceMerge Action Class"""
    def __init__(self, ilo, max_num_segments=None, delay=0):
        """
        :param ilo: An IndexList Object
        :param max_num_segments: Number of segments per shard to forceMerge
        :param delay: Number of seconds to delay between forceMerge operations

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type max_num_segments: int
        :type delay: int
        """
        verify_index_list(ilo)
        if not max_num_segments:
            raise MissingArgument('Missing value for "max_num_segments"')
        #: The :py:class:`~.curator.indexlist.IndexList` object passed from param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        #: Object attribute that gets the value of param ``max_num_segments``.
        self.max_num_segments = max_num_segments
        #: Object attribute that gets the value of param ``delay``.
        self.delay = delay
        self.loggit = logging.getLogger('curator.actions.forcemerge')

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        show_dry_run(
            self.index_list, 'forcemerge', max_num_segments=self.max_num_segments, delay=self.delay
        )

    def do_action(self):
        """:py:meth:`~.elasticsearch.client.IndicesClient.forcemerge` indices in :py:attr:`index_list`"""
        self.index_list.filter_closed()
        self.index_list.filter_forceMerged(max_num_segments=self.max_num_segments)
        self.index_list.empty_list_check()
        msg = (
            f'forceMerging {len(self.index_list.indices)} '
            f'selected indices: {self.index_list.indices}'
        )
        self.loggit.info(msg)
        try:
            for index_name in self.index_list.indices:
                msg = (
                    f'forceMerging index {index_name} to {self.max_num_segments} '
                    f'segments per shard. Please wait...'
                )
                self.loggit.info(msg)
                self.client.indices.forcemerge(
                    index=index_name, max_num_segments=self.max_num_segments)
                if self.delay > 0:
                    self.loggit.info('Pausing for %s seconds before continuing...', self.delay)
                    sleep(self.delay)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
