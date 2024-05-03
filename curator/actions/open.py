"""Open index action class"""
import logging
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import chunk_index_list, report_failure, show_dry_run, to_csv

class Open:
    """Open Action Class"""
    def __init__(self, ilo):
        """
        :param ilo: An IndexList Object

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        """
        verify_index_list(ilo)
        #: The :py:class:`~.curator.indexlist.IndexList` object passed from param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        self.loggit = logging.getLogger('curator.actions.open')

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        show_dry_run(self.index_list, 'open')

    def do_action(self):
        """:py:meth:`~.elasticsearch.client.IndicesClient.open` indices in :py:attr:`index_list`"""
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
