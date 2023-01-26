"""Forcemerge action class"""
import logging
from time import sleep
# pylint: disable=import-error
from curator.exceptions import MissingArgument
from curator.utils import report_failure, show_dry_run, verify_index_list

class ForceMerge:
    """ForceMerge Action Class"""
    def __init__(self, ilo, max_num_segments=None, delay=0):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg max_num_segments: Number of segments per shard to forceMerge
        :arg delay: Number of seconds to delay between forceMerge operations
        """
        verify_index_list(ilo)
        if not max_num_segments:
            raise MissingArgument('Missing value for "max_num_segments"')
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
        show_dry_run(
            self.index_list, 'forcemerge',
            max_num_segments=self.max_num_segments,
            delay=self.delay,
        )

    def do_action(self):
        """
        forcemerge indices in `index_list.indices`
        """
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
                    self.loggit.info(
                        'Pausing for %s seconds before continuing...', self.delay)
                    sleep(self.delay)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
