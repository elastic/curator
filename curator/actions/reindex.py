"""Reindex action class"""

import logging
from copy import deepcopy
from dotmap import DotMap  # type: ignore

# pylint: disable=broad-except, R0902,R0912,R0913,R0914,R0915
from es_client.builder import Builder
from es_client.helpers.utils import ensure_list, verify_url_schema
from es_client.exceptions import ConfigurationError
from curator.exceptions import CuratorException, FailedExecution, NoIndices

# Separate from es_client
from curator.exceptions import ConfigurationError as CuratorConfigError
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import report_failure
from curator.helpers.waiters import wait_for_it
from curator import IndexList


class Reindex:
    """Reindex Action Class"""

    def __init__(
        self,
        ilo,
        request_body,
        refresh=True,
        requests_per_second=-1,
        slices=1,
        timeout=60,
        wait_for_active_shards=1,
        wait_for_completion=True,
        max_wait=-1,
        wait_interval=9,
        remote_certificate=None,
        remote_client_cert=None,
        remote_client_key=None,
        remote_filters=None,
        migration_prefix='',
        migration_suffix='',
    ):
        """
        :param ilo: An IndexList Object
        :param request_body: The body to send to
            :py:meth:`~.elasticsearch.Elasticsearch.reindex`, which must be
            complete and usable, as Curator will do no vetting of the request_body.
            If it fails to function, Curator will return an exception.
        :param refresh: Whether to refresh the entire target index after the
            operation is complete.
        :param requests_per_second: The throttle to set on this request in
            sub-requests per second. ``-1`` means set no throttle as does
            ``unlimited`` which is the only non-float this accepts.
        :param slices: The number of slices this task  should be divided into.
            ``1`` means the task will not be sliced into subtasks. (Default: ``1``)
        :param timeout: The length in seconds each individual bulk request should
            wait for shards that are unavailable. (default: ``60``)
        :param wait_for_active_shards: Sets the number of shard copies that must be
            active before proceeding with the reindex operation. (Default: ``1``)
            means the primary shard only. Set to ``all`` for all shard copies,
            otherwise set to any non-negative value less than or equal to the total
            number of copies for the shard (number of replicas + 1)
        :param wait_for_completion: Wait for completion before returning.
        :param wait_interval: Seconds to wait between completion checks.
        :param max_wait: Maximum number of seconds to ``wait_for_completion``
        :param remote_certificate: Path to SSL/TLS certificate
        :param remote_client_cert: Path to SSL/TLS client certificate (public key)
        :param remote_client_key: Path to SSL/TLS private key
        :param migration_prefix: When migrating, prepend this value to the index name.
        :param migration_suffix: When migrating, append this value to the index name.

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type request_body: dict
        :type refresh: bool
        :type requests_per_second: int
        :type slices: int
        :type timeout: int
        :type wait_for_active_shards: int
        :type wait_for_completion: bool
        :type wait_interval: int
        :type max_wait: int
        :type remote_certificate: str
        :type remote_cclient_cert: str
        :type remote_cclient_key: str
        :type migration_prefix: str
        :type migration_suffix: str
        """
        if remote_filters is None:
            remote_filters = {}
        self.loggit = logging.getLogger('curator.actions.reindex')
        verify_index_list(ilo)
        if not isinstance(request_body, dict):
            raise CuratorConfigError('"request_body" is not of type dictionary')
        #: Object attribute that gets the value of param ``request_body``.
        self.body = request_body
        self.loggit.debug('REQUEST_BODY = %s', request_body)
        #: The :py:class:`~.curator.indexlist.IndexList` object passed from
        #: param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        #: Object attribute that gets the value of param ``refresh``.
        self.refresh = refresh
        #: Object attribute that gets the value of param ``requests_per_second``.
        self.requests_per_second = requests_per_second
        #: Object attribute that gets the value of param ``slices``.
        self.slices = slices
        #: Object attribute that gets the value of param ``timeout``, convert to
        #: :py:class:`str` and add ``s`` for seconds.
        self.timeout = f'{timeout}s'
        #: Object attribute that gets the value of param ``wait_for_active_shards``.
        self.wait_for_active_shards = wait_for_active_shards
        #: Object attribute that gets the value of param ``wait_for_completion``.
        self.wfc = wait_for_completion
        #: Object attribute that gets the value of param ``wait_interval``.
        self.wait_interval = wait_interval
        #: Object attribute that gets the value of param ``max_wait``.
        self.max_wait = max_wait
        #: Object attribute that gets the value of param ``migration_prefix``.
        self.mpfx = migration_prefix
        #: Object attribute that gets the value of param ``migration_suffix``.
        self.msfx = migration_suffix

        #: Object attribute that is set ``False`` unless :py:attr:`body` has
        #: ``{'source': {'remote': {}}}``, then it is set ``True``
        self.remote = False
        if 'remote' in self.body['source']:
            self.remote = True

        #: Object attribute that is set ``False`` unless :py:attr:`body` has
        #: ``{'dest': {'index': 'MIGRATION'}}``, then it is set ``True``
        self.migration = False
        if self.body['dest']['index'] == 'MIGRATION':
            self.migration = True

        if self.migration:
            if not self.remote and not self.mpfx and not self.msfx:
                raise CuratorConfigError(
                    'MIGRATION can only be used locally with one or both of '
                    'migration_prefix or migration_suffix.'
                )

        # REINDEX_SELECTION is the designated token.  If you use this for the
        # source "index," it will be replaced with the list of indices from the
        # provided 'ilo' (index list object).
        if self.body['source']['index'] == 'REINDEX_SELECTION' and not self.remote:
            self.body['source']['index'] = self.index_list.indices

        # Remote section
        elif self.remote:
            rclient_args = DotMap()
            rother_args = DotMap()
            self.loggit.debug('Remote reindex request detected')
            if 'host' not in self.body['source']['remote']:
                raise CuratorConfigError('Missing remote "host"')
            try:
                rclient_args.hosts = verify_url_schema(
                    self.body['source']['remote']['host']
                )
            except ConfigurationError as exc:
                raise CuratorConfigError(exc) from exc

            # Now that the URL schema is verified, these will pass.
            self.remote_host = rclient_args.hosts.split(':')[-2]
            self.remote_host = self.remote_host.split('/')[2]
            self.remote_port = rclient_args.hosts.split(':')[-1]

            if 'username' in self.body['source']['remote']:
                rother_args.username = self.body['source']['remote']['username']
            if 'password' in self.body['source']['remote']:
                rother_args.password = self.body['source']['remote']['password']
            if remote_certificate:
                rclient_args.ca_certs = remote_certificate
            if remote_client_cert:
                rclient_args.client_cert = remote_client_cert
            if remote_client_key:
                rclient_args.client_key = remote_client_key

            # Let's set a decent remote timeout for initially reading
            # the indices on the other side, and collecting their metadata
            rclient_args.request_timeout = 180

            # The rest only applies if using filters for remote indices
            if self.body['source']['index'] == 'REINDEX_SELECTION':
                self.loggit.debug('Filtering indices from remote')
                msg = (
                    f'Remote client args: '
                    f'hosts={rclient_args.hosts} '
                    f'username=REDACTED '
                    f'password=REDACTED '
                    f'certificate={remote_certificate} '
                    f'client_cert={remote_client_cert} '
                    f'client_key={remote_client_key} '
                    f'request_timeout={rclient_args.request_timeout} '
                    f'skip_version_test=True'
                )
                self.loggit.debug(msg)
                remote_config = {
                    'elasticsearch': {
                        'client': rclient_args.toDict(),
                        'other_settings': rother_args.toDict(),
                    }
                }
                try:  # let's try to build a remote connection with these!
                    builder = Builder(configdict=remote_config)
                    builder.version_min = (1, 0, 0)
                    builder.connect()
                    rclient = builder.client
                except Exception as err:
                    self.loggit.error(
                        'Unable to establish connection to remote Elasticsearch'
                        ' with provided credentials/certificates/settings.'
                    )
                    report_failure(err)
                try:
                    rio = IndexList(rclient)
                    rio.iterate_filters({'filters': remote_filters})
                    try:
                        rio.empty_list_check()
                    except NoIndices as exc:
                        raise FailedExecution(
                            'No actionable remote indices selected after applying '
                            'filters.'
                        ) from exc
                    self.body['source']['index'] = rio.indices
                except Exception as err:
                    self.loggit.error('Unable to get/filter list of remote indices.')
                    report_failure(err)

        self.loggit.debug('Reindexing indices: %s', self.body['source']['index'])

    def _get_request_body(self, source, dest):
        body = deepcopy(self.body)
        body['source']['index'] = source
        body['dest']['index'] = dest
        return body

    def _get_reindex_args(self, source, dest):
        # Always set wait_for_completion to False. Let 'wait_for_it' do its
        # thing if wait_for_completion is set to True. Report the task_id
        # either way.
        reindex_args = {
            'refresh': self.refresh,
            'requests_per_second': self.requests_per_second,
            'slices': self.slices,
            'timeout': self.timeout,
            'wait_for_active_shards': self.wait_for_active_shards,
            'wait_for_completion': False,
        }
        for keyname in [
            'dest',
            'source',
            'conflicts',
            'max_docs',
            'size',
            '_source',
            'script',
        ]:
            if keyname in self.body:
                reindex_args[keyname] = self.body[keyname]
        # Mimic the _get_request_body(source, dest) behavior by casting these values
        # here instead
        reindex_args['dest']['index'] = dest
        reindex_args['source']['index'] = source
        return reindex_args

    def get_processed_items(self, task_id):
        """
        This function calls :py:func:`~.elasticsearch.client.TasksClient.get` with
        the provided ``task_id``.  It will get the value from ``'response.total'``
        as the total number of elements processed during reindexing. If the value is
        not found, it will return ``-1``

        :param task_id: A task_id which ostensibly matches a task searchable in the
            tasks API.
        """
        try:
            task_data = self.client.tasks.get(task_id=task_id)
        except Exception as exc:
            raise CuratorException(
                f'Unable to obtain task information for task_id "{task_id}". '
                f'Exception {exc}'
            ) from exc
        total_processed_items = -1
        task = task_data['task']
        if task['action'] == 'indices:data/write/reindex':
            self.loggit.debug("It's a REINDEX TASK'")
            self.loggit.debug('TASK_DATA: %s', task_data)
            self.loggit.debug('TASK_DATA keys: %s', list(task_data.keys()))
            if 'response' in task_data:
                response = task_data['response']
                total_processed_items = response['total']
                self.loggit.debug('total_processed_items = %s', total_processed_items)
        return total_processed_items

    def _post_run_quick_check(self, index_name, task_id):
        # Check whether any documents were processed
        # if no documents processed, the target index "dest" won't exist
        processed_items = self.get_processed_items(task_id)
        if processed_items == 0:
            msg = (
                f'No items were processed. Will not check if target index '
                f'"{index_name}" exists'
            )
            self.loggit.info(msg)
        else:
            # Verify the destination index is there after the fact
            index_exists = self.client.indices.exists(index=index_name)
            alias_instead = self.client.indices.exists_alias(name=index_name)
            if not index_exists and not alias_instead:
                # pylint: disable=logging-fstring-interpolation
                self.loggit.error(
                    f'The index described as "{index_name}" was not found after the '
                    f'reindex operation. Check Elasticsearch logs for more '
                    f'information.'
                )
                if self.remote:
                    # pylint: disable=logging-fstring-interpolation
                    self.loggit.error(
                        f'Did you forget to add "reindex.remote.whitelist: '
                        f'{self.remote_host}:{self.remote_port}" to the '
                        f'elasticsearch.yml file on the "dest" node?'
                    )
                raise FailedExecution(
                    f'Reindex failed. The index or alias identified by "{index_name}" '
                    f'was not found.'
                )

    def sources(self):
        """Generator for Reindexing ``sources`` & ``dests``"""
        dest = self.body['dest']['index']
        source_list = ensure_list(self.body['source']['index'])
        self.loggit.debug('source_list: %s', source_list)
        if not source_list or source_list == ['REINDEX_SELECTED']:  # Empty list
            raise NoIndices
        if not self.migration:
            yield self.body['source']['index'], dest

        # Loop over all sources (default will only be one)
        else:
            for source in source_list:
                if self.migration:
                    dest = self.mpfx + source + self.msfx
                yield source, dest

    def show_run_args(self, source, dest):
        """Show what will run"""
        return (
            f'request body: {self._get_request_body(source, dest)} with arguments: '
            f'refresh={self.refresh} '
            f'requests_per_second={self.requests_per_second} '
            f'slices={self.slices} '
            f'timeout={self.timeout} '
            f'wait_for_active_shards={self.wait_for_active_shards} '
            f'wait_for_completion={self.wfc}'
        )

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        for source, dest in self.sources():
            self.loggit.info('DRY-RUN: REINDEX: %s', self.show_run_args(source, dest))

    def do_action(self):
        """
        Execute :py:meth:`~.elasticsearch.Elasticsearch.reindex` operation with the
        ``request_body`` from :py:meth:`_get_request_body` and arguments
        :py:attr:`refresh`, :py:attr:`requests_per_second`, :py:attr:`slices`,
        :py:attr:`timeout`, :py:attr:`wait_for_active_shards`, and :py:attr:`wfc`.
        """
        try:
            # Loop over all sources (default will only be one)
            for source, dest in self.sources():
                self.loggit.info('Commencing reindex operation')
                self.loggit.debug('REINDEX: %s', self.show_run_args(source, dest))
                response = self.client.reindex(**self._get_reindex_args(source, dest))

                self.loggit.debug('TASK ID = %s', response['task'])
                if self.wfc:
                    wait_for_it(
                        self.client,
                        'reindex',
                        task_id=response['task'],
                        wait_interval=self.wait_interval,
                        max_wait=self.max_wait,
                    )
                    self._post_run_quick_check(dest, response['task'])

                else:
                    msg = (
                        f'"wait_for_completion" set to {self.wfc}.  Remember to check '
                        f"task_id \"{response['task']}\" for successful completion "
                        f"manually."
                    )
                    self.loggit.warning(msg)
        except NoIndices as exc:
            raise NoIndices(
                'Source index must be list of actual indices. It must not be an empty '
                'list.'
            ) from exc
        except Exception as exc:
            report_failure(exc)
