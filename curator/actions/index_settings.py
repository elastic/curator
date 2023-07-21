"""Index settings action class"""
import logging
# pylint: disable=import-error
from curator.exceptions import ActionError, ConfigurationError, MissingArgument
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import chunk_index_list, report_failure, show_dry_run, to_csv

class IndexSettings:
    """Index Settings Action Class"""
    def __init__(
            self, ilo, index_settings=None, ignore_unavailable=False, preserve_existing=False):
        """
        :param ilo: An IndexList Object
        :param index_settings: A settings structure with one or more index settings to change.
        :param ignore_unavailable: Whether specified concrete indices should be ignored when
            unavailable (missing or closed)
        :param preserve_existing: Whether to update existing settings. If set to ``True``, existing
            settings on an index remain unchanged. The default is ``False``

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type index_settings: dict
        :type ignore_unavailable: bool
        :type preserve_existing: bool
        """
        if index_settings is None:
            index_settings = {}
        verify_index_list(ilo)
        if not index_settings:
            raise MissingArgument('Missing value for "index_settings"')
        #: The :py:class:`~.curator.indexlist.IndexList` object passed from param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        #: Object attribute that gets the value of param ``index_settings``.
        self.body = index_settings
        #: Object attribute that gets the value of param ``ignore_unavailable``.
        self.ignore_unavailable = ignore_unavailable
        #: Object attribute that gets the value of param ``preserve_existing``.
        self.preserve_existing = preserve_existing

        self.loggit = logging.getLogger('curator.actions.index_settings')
        self._body_check()

    def _body_check(self):
        # The body only passes the skimpiest of requirements by having 'index'
        # as the only root-level key, and having a 'dict' as its value
        if len(self.body) == 1:
            if 'index' in self.body:
                if isinstance(self.body['index'], dict):
                    return True
        raise ConfigurationError(f'Bad value for "index_settings": {self.body}')

    def _static_settings(self):
        return [
            'number_of_shards',
            'shard',
            'codec',
            'routing_partition_size',
        ]

    def _dynamic_settings(self):
        return [
            'number_of_replicas',
            'auto_expand_replicas',
            'refresh_interval',
            'max_result_window',
            'max_rescore_window',
            'blocks',
            'max_refresh_listeners',
            'mapping',
            'merge',
            'translog',
        ]

    def _settings_check(self):
        # Detect if even one index is open.  Save all found to open_index_list.
        open_index_list = []
        open_indices = False
        # This action requires index settings and state to be present
        # Calling these here should not cause undue problems, even if it's a repeat call
        self.index_list.get_index_state()
        self.index_list.get_index_settings()
        for idx in self.index_list.indices:
            if self.index_list.index_info[idx]['state'] == 'open':
                open_index_list.append(idx)
                open_indices = True
        for k in self.body['index']:
            if k in self._static_settings():
                if not self.ignore_unavailable:
                    if open_indices:
                        msg = (
                            f'Static Setting "{k}" detected with open indices: {open_index_list}. '
                            f'Static settings can only be used with closed indices.  Recommend '
                            f'filtering out open indices, or setting ignore_unavailable to True'
                        )
                        raise ActionError(msg)
            elif k in self._dynamic_settings():
                # Dynamic settings should be appliable to open or closed indices
                # Act here if the case is different for some settings.
                pass
            else:
                msg = f'"{k}" is not a setting Curator recognizes and may or may not work.'
                self.loggit.warning(msg)

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        show_dry_run(self.index_list, 'indexsettings', **self.body)

    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.IndicesClient.put_settings` in :py:attr:`body` to indices
        in :py:attr:`index_list`
        """
        self._settings_check()
        # Ensure that the open indices filter applied in _settings_check()
        # didn't result in an empty list (or otherwise empty)
        self.index_list.empty_list_check()
        msg = (
            f'Applying index settings to {len(self.index_list.indices)} indices: '
            f'{self.index_list.indices}'
        )
        self.loggit.info(msg)
        try:
            index_lists = chunk_index_list(self.index_list.indices)
            for lst in index_lists:
                response = self.client.indices.put_settings(
                    index=to_csv(lst), body=self.body,
                    ignore_unavailable=self.ignore_unavailable,
                    preserve_existing=self.preserve_existing
                )
                self.loggit.debug('PUT SETTINGS RESPONSE: %s', response)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
