"""Create index action class"""
import logging
# pylint: disable=import-error, broad-except
from elasticsearch8.exceptions import RequestError
from curator.exceptions import ConfigurationError, FailedExecution
from curator.utils import parse_date_pattern, report_failure

class CreateIndex:
    """Create Index Action Class"""
    def __init__(self, client, name=None, extra_settings=None, ignore_existing=False):
        """
        :arg client: An :class:`elasticsearch8.Elasticsearch` client object
        :arg name: A name, which can contain :py:func:`time.strftime`
            strings
        :arg extra_settings: The `settings` and `mappings` for the index. For
            more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/6.8/indices-create-index.html
        :type extra_settings: dict, representing the settings and mappings.
        :arg ignore_existing: If an index already exists, and this setting is ``True``,
            ignore the 400 error that results in a `resource_already_exists_exception` and
            return that it was successful.
        """
        if extra_settings is None:
            extra_settings = {}
        if not name:
            raise ConfigurationError('Value for "name" not provided.')
        #: Instance variable.
        #: The parsed version of `name`
        self.name = parse_date_pattern(name)
        #: Instance variable.
        #: Extracted from the action yaml, it should be a boolean informing
        #: whether to ignore the error if the index already exists.
        self.ignore_existing = ignore_existing
        #: Instance variable.
        #: An :class:`elasticsearch8.Elasticsearch` client object
        self.client = client

        self.extra_settings = extra_settings
        self.aliases = None
        self.mappings = None
        self.settings = None

        if 'aliases' in extra_settings:
            self.aliases = extra_settings.pop('aliases')
        if 'mappings' in extra_settings:
            self.mappings = extra_settings.pop('mappings')
        if 'settings' in extra_settings:
            self.settings = extra_settings.pop('settings')

        self.loggit = logging.getLogger('curator.actions.create_index')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        msg = f'DRY-RUN: create_index "{self.name}" with arguments: {self.extra_settings}'
        self.loggit.info(msg)

    def do_action(self):
        """
        Create index identified by `name` with settings in `extra_settings`
        """
        msg = (f'Creating index "{self.name}" with settings: {self.extra_settings}')
        self.loggit.info(msg)
        try:
            # self.client.indices.create(index=self.name, body=self.body)
            self.client.indices.create(index=self.name, aliases=self.aliases, mappings=self.mappings, settings=self.settings)
        # Most likely error is a 400, `resource_already_exists_exception`
        except RequestError as err:
            match_list = ["index_already_exists_exception", "resource_already_exists_exception"]
            if err.error in match_list and self.ignore_existing:
                self.loggit.warning('Index %s already exists.', self.name)
            else:
                raise FailedExecution(f'Index {self.name} already exists.') from err
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
