"""Close index action class"""
import logging
# pylint: disable=import-error
from curator.utils import chunk_index_list, report_failure, show_dry_run, to_csv, verify_index_list

class Close:
    """Close Action Class"""
    def __init__(self, ilo, delete_aliases=False, skip_flush=False):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg delete_aliases: If `True`, will delete any associated aliases
            before closing indices.
        :type delete_aliases: bool
        :arg skip_flush: If `True`, will not flush indices before closing.
        :type skip_flush: bool
        """
        verify_index_list(ilo)
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internal reference to `delete_aliases`
        self.delete_aliases = delete_aliases
        #: Instance variable.
        #: Internal reference to `skip_flush`
        self.skip_flush = skip_flush
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        self.loggit = logging.getLogger('curator.actions.close')


    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        show_dry_run(
            self.index_list, 'close', **{'delete_aliases':self.delete_aliases})

    def do_action(self):
        """
        Close open indices in `index_list.indices`
        """
        self.index_list.filter_closed()
        self.index_list.empty_list_check()
        self.loggit.info(
            'Closing %s selected indices: %s',
                len(self.index_list.indices), self.index_list.indices
        )
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                lst_as_csv = to_csv(lst)
                self.loggit.debug('CSV list of indices to close:  %s', lst_as_csv)
                if self.delete_aliases:
                    self.loggit.info('Deleting aliases from indices before closing.')
                    self.loggit.debug('Deleting aliases from:  %s', lst)
                    try:
                        self.client.indices.delete_alias(index=lst_as_csv, name='_all')
                        self.loggit.debug('Deleted aliases from: %s', lst)
                    # pylint: disable=broad-except
                    except Exception as err:
                        self.loggit.warning(
                            'Some indices may not have had aliases.  Exception: %s', err)
                if not self.skip_flush:
                    self.client.indices.flush(index=lst_as_csv, ignore_unavailable=True, force=True)
                self.client.indices.close(index=lst_as_csv, ignore_unavailable=True)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
