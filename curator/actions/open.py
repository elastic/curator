"""Open index action class"""
import logging
# pylint: disable=import-error
from curator.utils import chunk_index_list, report_failure, show_dry_run, to_csv, verify_index_list

class Open:
    """Open Action Class"""
    def __init__(self, ilo):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        """
        verify_index_list(ilo)
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        self.loggit = logging.getLogger('curator.actions.open')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        show_dry_run(self.index_list, 'open')

    def do_action(self):
        """
        Open closed indices in `index_list.indices`
        """
        self.index_list.empty_list_check()
        msg = f'Opening {len(self.index_list.indices)} selected indices: {self.index_list.indices}'
        self.loggit.info(msg)
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                self.client.indices.open(index=to_csv(lst))
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
