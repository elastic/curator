"""Alias action class"""
import logging
# pylint: disable=import-error
from curator.exceptions import ActionError, MissingArgument, NoIndices
from curator.utils import parse_date_pattern, parse_datemath, report_failure, verify_index_list

class Alias:
    """Alias Action Class"""
    # pylint: disable=unused-argument
    def __init__(self, name=None, extra_settings=None, **kwargs):
        """
        Define the Alias object.

        :arg name: The alias name
        :arg extra_settings: Extra settings, including filters and routing. For
            more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/8.6/indices-aliases.html
        :type extra_settings: dict, representing the settings.
        """
        if extra_settings is None:
            extra_settings = {}
        if not name:
            raise MissingArgument('No value for "name" provided.')
        #: Instance variable
        #: The strftime parsed version of `name`.
        self.name = parse_date_pattern(name)
        #: The list of actions to perform.  Populated by
        #: :mod:`curator.actions.Alias.add` and
        #: :mod:`curator.actions.Alias.remove`
        self.actions = []
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = None
        #: Instance variable.
        #: Any extra things to add to the alias, like filters, or routing.
        self.extra_settings = extra_settings
        self.loggit = logging.getLogger('curator.actions.alias')
        #: Instance variable.
        #: Preset default value to `False`.
        self.warn_if_no_indices = False

    def add(self, ilo, warn_if_no_indices=False):
        """
        Create `add` statements for each index in `ilo` for `alias`, then
        append them to `actions`.  Add any `extras` that may be there.

        :arg ilo: A :class:`curator.indexlist.IndexList` object

        """
        verify_index_list(ilo)
        self.loggit.debug('ADD -> ILO = %s', ilo)
        if not self.client:
            self.client = ilo.client
        self.name = parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except NoIndices as exc:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                self.loggit.warning(
                    'No indices found after processing filters. '
                    'Nothing to add to %s', self.name
                )
                return

            # Re-raise the exceptions.NoIndices so it will behave as before
            raise NoIndices('No indices to add to alias') from exc
        for index in ilo.working_list():
            self.loggit.debug(
                'Adding index %s to alias %s with extra settings '
                '%s', index, self.name, self.extra_settings
            )
            add_dict = {'add' : {'index' : index, 'alias': self.name}}
            add_dict['add'].update(self.extra_settings)
            self.actions.append(add_dict)

    def remove(self, ilo, warn_if_no_indices=False):
        """
        Create `remove` statements for each index in `ilo` for `alias`,
        then append them to `actions`.

        :arg ilo: A :class:`curator.indexlist.IndexList` object
        """
        verify_index_list(ilo)
        self.loggit.debug('REMOVE -> ILO = %s', ilo)
        if not self.client:
            self.client = ilo.client
        self.name = parse_datemath(self.client, self.name)
        try:
            ilo.empty_list_check()
        except NoIndices as exc:
            # Add a warning if there are no indices to add, if so set in options
            if warn_if_no_indices:
                self.warn_if_no_indices = True
                self.loggit.warning(
                    'No indices found after processing filters. '
                    'Nothing to remove from %s', self.name
                )
                return

            # Re-raise the exceptions.NoIndices so it will behave as before
            raise NoIndices('No indices to remove from alias') from exc
        aliases = self.client.indices.get_alias()
        for index in ilo.working_list():
            if index in aliases:
                self.loggit.debug('Index %s in get_aliases output', index)
                # Only remove if the index is associated with the alias
                if self.name in aliases[index]['aliases']:
                    self.loggit.debug('Removing index %s from alias %s', index, self.name)
                    self.actions.append(
                        {'remove' : {'index' : index, 'alias': self.name}})
                else:
                    self.loggit.debug(
                        'Can not remove: Index %s is not associated with alias %s', index, self.name
                    )

    def check_actions(self):
        """
        Return `self.actions` for use with the `update_aliases` API call if actions exist,
        otherwise raise an exception
        """
        if not self.actions:
            if not self.warn_if_no_indices:
                raise ActionError('No "add" or "remove" operations')

            raise NoIndices('No "adds" or "removes" found.  Taking no action')
        self.loggit.debug('Alias actions: %s', self.actions)

        return self.actions

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
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
            self.loggit.info(msg)

    def do_action(self):
        """
        Run the API call `update_aliases` with the results of `body()`
        """
        self.loggit.info('Updating aliases...')
        self.loggit.info('Alias actions: %s', self.actions)
        try:
            self.client.indices.update_aliases(actions=self.actions)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
