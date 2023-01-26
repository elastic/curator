"""Delete index action class"""
import logging
# pylint: disable=import-error
from curator.utils import (
    chunk_index_list, get_indices, report_failure, show_dry_run, to_csv, verify_index_list)


class DeleteIndices:
    """Delete Indices Action Class"""
    def __init__(self, ilo, master_timeout=30):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg master_timeout: Number of seconds to wait for master node response
        """
        verify_index_list(ilo)
        if not isinstance(master_timeout, int):
            raise TypeError(
                f'Incorrect type for "master_timeout": {type(master_timeout)}. '
                f'Should be integer value.'
            )
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: String value of `master_timeout` + 's', for seconds.
        self.master_timeout = str(master_timeout) + 's'
        self.loggit = logging.getLogger('curator.actions.delete_indices')
        self.loggit.debug('master_timeout value: %s', self.master_timeout)

    def _verify_result(self, result, count):
        """
        Breakout method to aid readability
        :arg result: A list of indices from `_get_result_list`
        :arg count: The number of tries that have occurred
        :rtype: bool
        """
        if isinstance(result, list) and result:
            self.loggit.error(
                'The following indices failed to delete on try #%s:', count)
            for idx in result:
                self.loggit.error("---%s",idx)
            retval = False
        else:
            self.loggit.debug('Successfully deleted all indices on try #%s', count)
            retval = True
        return retval

    def __chunk_loop(self, chunk_list):
        """
        Loop through deletes 3 times to ensure they complete
        :arg chunk_list: A list of indices pre-chunked so it won't overload the
            URL size limit.
        """
        working_list = chunk_list
        for count in range(1, 4): # Try 3 times
            for i in working_list:
                self.loggit.info("---deleting index %s", i)
            self.client.indices.delete(
                index=to_csv(working_list), master_timeout=self.master_timeout)
            result = [i for i in working_list if i in get_indices(self.client)]
            if self._verify_result(result, count):
                return
            working_list = result
        self.loggit.error(
            'Unable to delete the following indices after 3 attempts: %s', result)

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        show_dry_run(self.index_list, 'delete_indices')

    def do_action(self):
        """
        Delete indices in `index_list.indices`
        """
        self.index_list.empty_list_check()
        msg = (
            f'Deleting {len(self.index_list.indices)} '
            f'selected indices: {self.index_list.indices}'
        )
        self.loggit.info(msg)
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                self.__chunk_loop(lst)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
