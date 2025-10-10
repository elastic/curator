"""Alias action"""

import logging

# pylint: disable=import-error
from curator.debug import debug, begin_end
from curator.exceptions import ActionError, MissingArgument, NoIndices
from curator.helpers.date_ops import parse_date_pattern, parse_datemath
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import report_failure

logger = logging.getLogger(__name__)


class Alias:
    """Alias Action Class"""

    # pylint: disable=unused-argument
    def __init__(self, name=None, extra_settings=None, **kwargs):
        """
        :param name: The alias name
        :param extra_settings: Extra settings, including filters and routing.
            For more information see `here
            </https://www.elastic.co/guide/en/elasticsearch/reference/8.6/indices-aliases.html>`_.

        :type name: str
        :type extra_settings: dict
        """
        if extra_settings is None:
            extra_settings = {}
        if not name:
            raise MissingArgument('No value for "name" provided.')
        #: The :py:func:`~.curator.helpers.date_ops.parse_date_pattern` rendered
        #: version of what was passed by param ``name``.
        self.name = parse_date_pattern(name)
        #: The list of actions to perform.  Populated by
        #: :py:meth:`~.curator.actions.Alias.add` and
        #: :py:meth:`~.curator.actions.Alias.remove`
        self.actions = []
        #: Any extra things to add to the alias, like filters, or routing. Gets
        #: the value from param ``extra_settings``.
        self.extra_settings = extra_settings
        #: Preset default value to ``False``.
        self.warn_if_no_indices = False

    @property
    def client(self):
        """The :py:class:`~.elasticsearch.Elasticsearch` client object"""
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    @begin_end()
    def add(self, ilo, warn_if_no_indices=False):
        """
        Create ``add`` statements for each index in ``ilo`` for :py:attr:`name`, then
        append them to :py:attr:`actions`.  Add any :py:attr:`extra_settings` that
        may be there.

        :param ilo: An IndexList Object
        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        """
        verify_index_list(ilo)
        debug.lv5('ADD -> ILO = %s', ilo.indices)
        self.client = ilo.client
        self.name = parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except NoIndices as exc:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                logger.warning(
                    'No indices found after processing filters. Nothing to add to %s',
                    self.name,
                )
                return
            # Re-raise the exceptions.NoIndices so it will behave as before
            raise NoIndices('No indices to add to alias') from exc
        for index in ilo.working_list():
            debug.lv1(
                'Adding index %s to alias %s with extra settings %s',
                index,
                self.name,
                self.extra_settings,
            )
            add_dict = {'add': {'index': index, 'alias': self.name}}
            add_dict['add'].update(self.extra_settings)
            self.actions.append(add_dict)

    @begin_end()
    def remove(self, ilo, warn_if_no_indices=False):
        """
        Create ``remove`` statements for each index in ``ilo`` for :py:attr:`name`,
        then append them to :py:attr:`actions`.

        :param ilo: An IndexList Object
        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        """
        verify_index_list(ilo)
        debug.lv5('REMOVE -> ILO = %s', ilo.indices)
        self.client = ilo.client
        self.name = parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except NoIndices as exc:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                logger.warning(
                    'No indices found after processing filters. '
                    'Nothing to remove from %s',
                    self.name,
                )
                return

            # Re-raise the exceptions.NoIndices so it will behave as before
            raise NoIndices('No indices to remove from alias') from exc
        aliases = self.client.indices.get_alias(expand_wildcards=['open', 'closed'])
        for index in ilo.working_list():
            if index in aliases:
                debug.lv3('Index %s in get_aliases output', index)
                # Only remove if the index is associated with the alias
                if self.name in aliases[index]['aliases']:
                    debug.lv1('Removing index %s from alias %s', index, self.name)
                    self.actions.append(
                        {'remove': {'index': index, 'alias': self.name}}
                    )
                else:
                    debug.lv2(
                        'Can not remove: Index %s is not associated with alias %s',
                        index,
                        self.name,
                    )

    @begin_end()
    def check_actions(self):
        """
        :returns: :py:attr:`actions` for use with the
            :py:meth:`~.elasticsearch.client.IndicesClient.update_aliases` API
            call if actions exist, otherwise an exception is raised.
        """
        if not self.actions:
            if not self.warn_if_no_indices:
                raise ActionError('No "add" or "remove" operations')
            raise NoIndices('No "adds" or "removes" found.  Taking no action')
        debug.lv5('Alias actions: %s', self.actions)

        return self.actions

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        logger.info('DRY-RUN MODE.  No changes will be made.')
        for item in self.check_actions():
            job = list(item.keys())[0]
            index = item[job]['index']
            alias = item[job]['alias']
            # We want our log to look clever, so if job is "remove", strip the
            # 'e' so "remove" can become "removing".  "adding" works already.
            msg = (
                f"DRY-RUN: alias: {job.rstrip('e')}ing index \"{index}\" "
                f"{'to' if job == 'add' else 'from'} alias \"{alias}\""
            )
            logger.info(msg)

    @begin_end()
    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.IndicesClient.update_aliases` for
        :py:attr:`name` with :py:attr:`actions`
        """
        logger.info('Updating aliases...')
        debug.lv3('Alias actions: %s', self.actions)
        try:
            self.client.indices.update_aliases(actions=self.actions)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
