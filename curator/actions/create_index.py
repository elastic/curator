"""Create index action class"""
import logging
# pylint: disable=import-error, broad-except
from elasticsearch8.exceptions import RequestError
from curator.exceptions import ConfigurationError, FailedExecution
from curator.helpers.date_ops import parse_date_pattern
from curator.helpers.utils import report_failure

class CreateIndex:
    """Create Index Action Class"""
    def __init__(self, client, name=None, extra_settings=None, ignore_existing=False):
        """
        :param client: A client connection object
        :param name: A name, which can contain :py:func:`time.strftime` strings
        :param extra_settings: The `settings` and `mappings` for the index. For more information
            see `the create indices documentation </https://www.elastic.co/guide/en/elasticsearch/reference/8.6/indices-create-index.html>`_.
        :param ignore_existing: If an index already exists, and this setting is ``True``, ignore
            the 400 error that results in a ``resource_already_exists_exception`` and return that
            it was successful.

        :type client: :py:class:`~.elasticsearch.Elasticsearch`
        :type name: str
        :type extra_settings: dict
        :type ignore_existing: bool
        """
        if extra_settings is None:
            extra_settings = {}
        if not name:
            raise ConfigurationError('Value for "name" not provided.')
        #: The :py:func:`~.curator.helpers.date_ops.parse_date_pattern` rendered
        #: version of what was passed as ``name``.
        self.name = parse_date_pattern(name)
        #: Extracted from the action definition, it should be a boolean informing
        #: whether to ignore the error if the index already exists.
        self.ignore_existing = ignore_existing
        #: An :py:class:`~.elasticsearch.Elasticsearch` client object
        self.client = client
        #: Any extra settings for the index, like aliases, mappings, or settings. Gets the value
        #: from param ``extra_settings``.
        self.extra_settings = extra_settings
        #: Gets any ``aliases`` from :py:attr:`extra_settings` or is :py:class:`None`
        self.aliases = None
        #: Gets any ``mappings`` from :py:attr:`extra_settings` or is :py:class:`None`
        self.mappings = None
        #: Gets any ``settings`` from :py:attr:`extra_settings` or is :py:class:`None`
        self.settings = None

        if 'aliases' in extra_settings:
            self.aliases = extra_settings.pop('aliases')
        if 'mappings' in extra_settings:
            self.mappings = extra_settings.pop('mappings')
        if 'settings' in extra_settings:
            self.settings = extra_settings.pop('settings')
        self.loggit = logging.getLogger('curator.actions.create_index')

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        msg = f'DRY-RUN: create_index "{self.name}" with arguments: {self.extra_settings}'
        self.loggit.info(msg)

    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.IndicesClient.create` index identified by :py:attr:`name`
        with values from :py:attr:`aliases`, :py:attr:`mappings`, and :py:attr:`settings`
        """
        msg = f'Creating index "{self.name}" with settings: {self.extra_settings}'
        self.loggit.info(msg)
        try:
            self.client.indices.create(
                index=self.name, aliases=self.aliases, mappings=self.mappings,
                settings=self.settings
            )
        # Most likely error is a 400, `resource_already_exists_exception`
        except RequestError as err:
            match_list = ["index_already_exists_exception", "resource_already_exists_exception"]
            if err.error in match_list:
                if self.ignore_existing:
                    self.loggit.warning('Index %s already exists.', self.name)
                else:
                    raise FailedExecution(f'Index {self.name} already exists.') from err
            else:
                msg = f'Unable to create index "{self.name}". Error: {err.error}'
                raise FailedExecution(msg) from err
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
