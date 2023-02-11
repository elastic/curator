"""Snapshot and Restore action classes"""
import logging
import re
from es_client.helpers.utils import ensure_list
from curator.helpers.date_ops import parse_datemath, parse_date_pattern
from curator.helpers.getters import get_indices
from curator.helpers.testers import (
    repository_exists, snapshot_running, verify_index_list, verify_repository, verify_snapshot_list
)
from curator.helpers.utils import report_failure, to_csv
from curator.helpers.waiters import wait_for_it
# pylint: disable=broad-except
from curator.exceptions import (
        ActionError, CuratorException, FailedRestore, FailedSnapshot, MissingArgument,
        SnapshotInProgress
    )

class Snapshot(object):
    """Snapshot Action Class

    Read more about identically named settings at:
    :py:meth:`elasticsearch.client.SnapshotClient.create`
    """
    def __init__(self, ilo, repository=None, name=None, ignore_unavailable=False,
        include_global_state=True, partial=False, wait_for_completion=True, wait_interval=9,
        max_wait=-1, skip_repo_fs_check=True
    ):
        """
        :param ilo: An IndexList Object
        :param repository: Repository name.
        :param name: Snapshot name.
        :param ignore_unavailable: Ignore unavailable shards/indices.
        :param include_global_state: Store cluster global state with snapshot.
        :param partial: Do not fail if primary shard is unavailable.
        :param wait_for_completion: Wait for completion before returning.
        :param wait_interval: Seconds to wait between completion checks.
        :param max_wait: Maximum number of seconds to ``wait_for_completion``
        :param skip_repo_fs_check: Do not validate write access to repository on all cluster nodes
            before proceeding. Useful for shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success. (Default: ``True``)

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type repository: str
        :type name: str
        :type ignore_unavailable: bool
        :type include_global_state: bool
        :type partial: bool
        :type wait_for_completion: bool
        :type wait_interval: int
        :type max_wait: int
        :type skip_repo_fs_check: bool
        """
        verify_index_list(ilo)
        # Check here and don't bother with the rest of this if there are no
        # indices in the index list.
        ilo.empty_list_check()
        if not repository_exists(ilo.client, repository=repository):
            raise ActionError(
                f'Cannot snapshot indices to missing repository: {repository}')
        if not name:
            raise MissingArgument('No value for "name" provided.')
        #: The :py:class:`~.curator.indexlist.IndexList` object passed from param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        #: The :py:func:`~.curator.helpers.date_ops.parse_date_pattern` rendered
        #: version of what was passed by param ``name``.
        self.name = parse_datemath(self.client, parse_date_pattern(name))
        #: Object attribute that gets the value of param ``repository``.
        self.repository = repository
        #: Object attribute that gets the value of param ``wait_for_completion``.
        self.wait_for_completion = wait_for_completion
        #: Object attribute that gets the value of param ``wait_interval``.
        self.wait_interval = wait_interval
        #: Object attribute that gets the value of param ``max_wait``.
        self.max_wait = max_wait
        #: Object attribute that gets the value of param ``skip_repo_fs_check``.
        self.skip_repo_fs_check = skip_repo_fs_check
        #: Object attribute that tracks the snapshot state.
        self.state = None
        #: Object attribute that contains the :py:func:`~.curator.helpers.utils.to_csv` output of
        #: the indices in :py:attr:`index_list`.
        self.indices = to_csv(ilo.indices)
        #: Object attribute that gets the value of param ``ignore_unavailable``.
        self.ignore_unavailable = ignore_unavailable
        #: Object attribute that gets the value of param ``include_global_state``.
        self.include_global_state = include_global_state
        #: Object attribute that gets the value of param ``partial``.
        self.partial = partial
        #: Object attribute dictionary compiled from :py:attr:`indices`,
        #: :py:attr:`ignore_unavailable`, :py:attr:`include_global_state`, and :py:attr:`partial`
        self.settings = {
            'indices': ilo.indices,
            'ignore_unavailable': self.ignore_unavailable,
            'include_global_state': self.include_global_state,
            'partial': self.partial
        }

        self.loggit = logging.getLogger('curator.actions.snapshot')

    def get_state(self):
        """Get the state of the snapshot and set :py:attr:`state`"""
        try:
            self.state = self.client.snapshot.get(
                repository=self.repository, snapshot=self.name)['snapshots'][0]['state']
            return self.state
        except IndexError as exc:
            raise CuratorException(
                f'Snapshot "{self.name}" not found in repository "{self.repository}"') from exc

    def report_state(self):
        """
        Log the :py:attr:`state` of the snapshot and raise :py:exc:`FailedSnapshot` if
        :py:attr:`state` is not ``SUCCESS``
        """
        self.get_state()
        if self.state == 'SUCCESS':
            self.loggit.info('Snapshot %s successfully completed.', self.name)
        else:
            msg = f'Snapshot {self.name} completed with state: {self.state}'
            self.loggit.error(msg)
            raise FailedSnapshot(msg)

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        msg = (
            f'DRY-RUN: snapshot: {self.name} in repository {self.repository} '
            f'with arguments: {self.settings}'
        )
        self.loggit.info(msg)

    def do_action(self):
        """
        :py:meth:`elasticsearch.client.SnapshotClient.create` a snapshot of :py:attr:`indices`,
        with passed parameters.
        """
        if not self.skip_repo_fs_check:
            verify_repository(self.client, self.repository)
        if snapshot_running(self.client):
            raise SnapshotInProgress('Snapshot already in progress.')
        try:
            self.loggit.info(
                'Creating snapshot "%s" from indices: %s', self.name, self.index_list.indices)
            # Always set wait_for_completion to False. Let 'wait_for_it' do its
            # thing if wait_for_completion is set to True. Report the task_id
            # either way.
            self.client.snapshot.create(
                repository=self.repository,
                snapshot=self.name,
                ignore_unavailable=self.ignore_unavailable,
                include_global_state=self.include_global_state,
                indices=self.indices,
                partial=self.partial,
                wait_for_completion=False
            )
            if self.wait_for_completion:
                wait_for_it(
                    self.client, 'snapshot', snapshot=self.name,
                    repository=self.repository,
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
                self.report_state()
            else:
                msg = (
                    f'"wait_for_completion" set to {self.wait_for_completion}. '
                    f'Remember to check for successful completion manually.'
                )
                self.loggit.warning(msg)
        except Exception as err:
            report_failure(err)

class DeleteSnapshots:
    """Delete Snapshots Action Class"""
    def __init__(self, slo, retry_interval=120, retry_count=3):
        """
        :param slo: A SnapshotList object
        :type slo: :py:class:`~.curator.snapshotlist.SnapshotList`
        :param retry_interval: Seconds to delay betwen retries. (Default: ``120``)
        :type retry_interval: int
        :param retry_count: Number of attempts to make. (Default: ``3``)
        :type retry_count: int
        """
        verify_snapshot_list(slo)
        #: The :py:class:`~.curator.snapshotlist.SnapshotList` object passed from param ``slo``
        self.snapshot_list = slo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`snapshot_list`
        self.client = slo.client
        #: Object attribute that gets the value of param ``retry_interval``.
        self.retry_interval = retry_interval
        #: Object attribute that gets the value of param ``retry_count``.
        self.retry_count = retry_count
        #: Object attribute that gets its value from :py:attr:`snapshot_list`.
        self.repository = slo.repository
        self.loggit = logging.getLogger('curator.actions.delete_snapshots')

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        mykwargs = {
            'repository' : self.repository,
            'retry_interval' : self.retry_interval,
            'retry_count' : self.retry_count,
        }
        for snap in self.snapshot_list.snapshots:
            self.loggit.info('DRY-RUN: delete_snapshot: %s with arguments: %s', snap, mykwargs)

    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.SnapshotClient.delete` snapshots in
        :py:attr:`snapshot_list`. Retry up to :py:attr:`retry_count` times, pausing
        :py:attr:`retry_interval` seconds between retries.
        """
        self.snapshot_list.empty_list_check()
        msg = (
            f'Deleting {len(self.snapshot_list.snapshots)} '
            f'selected snapshots: {self.snapshot_list.snapshots}'
        )
        self.loggit.info(msg)
        try:
            for snap in self.snapshot_list.snapshots:
                self.loggit.info('Deleting snapshot %s...', snap)
                self.client.snapshot.delete(repository=self.repository, snapshot=snap)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)

class Restore(object):
    """Restore Action Class

    Read more about identically named settings at:
    :py:meth:`elasticsearch.client.SnapshotClient.restore`
    """
    def __init__(
            self, slo, name=None, indices=None, include_aliases=False, ignore_unavailable=False,
            include_global_state=False, partial=False, rename_pattern=None,
            rename_replacement=None, extra_settings=None, wait_for_completion=True, wait_interval=9,
            max_wait=-1, skip_repo_fs_check=True
    ):
        """
        :param slo: A SnapshotList object
        :param name: Name of the snapshot to restore.  If ``None``, use the most recent snapshot.
        :param indices: Indices to restore.  If ``None``, all in the snapshot will be restored.
        :param include_aliases: Restore aliases with the indices.
        :param ignore_unavailable: Ignore unavailable shards/indices.
        :param include_global_state: Restore cluster global state with snapshot.
        :param partial: Do not fail if primary shard is unavailable.
        :param rename_pattern: A regular expression pattern with one or more captures, e.g.
            ``index_(.+)``
        :param rename_replacement: A target index name pattern with `$#` numbered references to the
            captures in ``rename_pattern``, e.g. ``restored_index_$1``
        :param extra_settings: Index settings to apply to restored indices.
        :param wait_for_completion: Wait for completion before returning.
        :param wait_interval: Seconds to wait between completion checks.
        :param max_wait: Maximum number of seconds to ``wait_for_completion``
        :param skip_repo_fs_check: Do not validate write access to repository on all cluster nodes
            before proceeding. Useful for shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success. (Default: ``True``)

        :type slo: :py:class:`~.curator.snapshotlist.SnapshotList`
        :type name: str
        :type indices: list
        :type include_aliases: bool
        :type ignore_unavailable: bool
        :type include_global_state: bool
        :type partial: bool
        :type rename_pattern: str
        :type rename_replacement: str
        :type extra_settings: dict
        :type wait_for_completion: bool
        :type wait_interval: int
        :type max_wait: int
        :type skip_repo_fs_check: bool
        """
        if extra_settings is None:
            extra_settings = {}
        self.loggit = logging.getLogger('curator.actions.snapshot')
        verify_snapshot_list(slo)
        # Get the most recent snapshot.
        most_recent = slo.most_recent()
        self.loggit.debug('"most_recent" snapshot: %s', most_recent)
        #: Object attribute that gets the value of param ``name`` if not ``None``, or the output
        #: from :py:meth:`~.curator.SnapshotList.most_recent`.
        self.name = name if name else most_recent
        # Stop here now, if it's not a successful snapshot.
        if slo.snapshot_info[self.name]['state'] == 'PARTIAL' and partial:
            self.loggit.warning('Performing restore of snapshot in state PARTIAL.')
        elif slo.snapshot_info[self.name]['state'] != 'SUCCESS':
            raise CuratorException(
                'Restore operation can only be performed on snapshots with '
                'state "SUCCESS", or "PARTIAL" if partial=True.'
            )

        #: Internal reference to `slo`
        self.snapshot_list = slo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`snapshot_list`
        self.client = slo.client
        #: Object attribute that gets the value of ``repository`` from :py:attr:`snapshot_list`.
        self.repository = slo.repository

        if indices:
            self.indices = ensure_list(indices)
        else:
            self.indices = slo.snapshot_info[self.name]['indices']
        #: Object attribute that gets the value of param ``wait_for_completion``.
        self.wfc = wait_for_completion
        #: Object attribute that gets the value of param ``wait_interval``.
        self.wait_interval = wait_interval
        #: Object attribute that gets the value of param ``max_wait``.
        self.max_wait = max_wait
        #: Object attribute that gets the value of param ``rename_pattern``. Empty :py:class:`str`
        #: if ``None``
        self.rename_pattern = rename_pattern if rename_replacement is not None \
            else ''
        #: Object attribute that gets the value of param ``rename_replacement``. Empty
        #: :py:class:`str` if ``None``
        self.rename_replacement = rename_replacement if rename_replacement \
            is not None else ''
        #: Object attribute derived from :py:attr:`rename_replacement`. but with Java regex group
        #: designations of ``$#`` converted to Python's ``\\#`` style.
        self.py_rename_replacement = self.rename_replacement.replace('$', '\\')
        #: Object attribute that gets the value of param ``max_wait``.
        self.skip_repo_fs_check = skip_repo_fs_check

        #: Object attribute that gets populated from other params/attributes. Deprecated, but not
        #: removed. Lazy way to keep from updating :py:meth:`do_dry_run`. Will fix later.
        self.body = {
            'indices' : self.indices,
            'include_aliases' : include_aliases,
            'ignore_unavailable' : ignore_unavailable,
            'include_global_state' : include_global_state,
            'partial' : partial,
            'rename_pattern' : self.rename_pattern,
            'rename_replacement' : self.rename_replacement,
        }
        #: Object attribute that gets the value of param ``include_aliases``.
        self.include_aliases = include_aliases
        #: Object attribute that gets the value of param ``ignore_unavailable``.
        self.ignore_unavailable = ignore_unavailable
        #: Object attribute that gets the value of param ``include_global_state``.
        self.include_global_state = include_global_state
        #: Object attribute that gets the value of param ``include_aliases``.
        self.include_aliases = include_aliases
        #: Object attribute that gets the value of param ``partial``.
        self.partial = partial
        #: Object attribute that gets the value of param ``extra_settings``.
        self.index_settings = None

        if extra_settings:
            self.loggit.debug('Adding extra_settings to restore body: %s',extra_settings)
            self.index_settings = extra_settings
            try:
                self.body.update(extra_settings)
            except Exception:
                self.loggit.error('Unable to apply extra settings to restore body')
        self.loggit.debug('REPOSITORY: %s', self.repository)
        self.loggit.debug('WAIT_FOR_COMPLETION: %s', self.wfc)
        self.loggit.debug('SKIP_REPO_FS_CHECK: %s', self.skip_repo_fs_check)
        self.loggit.debug('BODY: %s', self.body)
        # Populate the expected output index list.
        self._get_expected_output()

    def _get_expected_output(self):
        if not self.rename_pattern and not self.rename_replacement:
            self.expected_output = self.indices
            return # Don't stick around if we're not replacing anything
        self.expected_output = []
        for index in self.indices:
            self.expected_output.append(
                re.sub(self.rename_pattern, self.py_rename_replacement, index)
            )
            msg = f'index: {index} replacement: {self.expected_output[-1]}'
            self.loggit.debug(msg)

    def report_state(self):
        """
        Log the state of the restore. This should only be done if ``wait_for_completion`` is
        ``True``, and only after completing the restore.
        """
        all_indices = get_indices(self.client)
        found_count = 0
        missing = []
        for index in self.expected_output:
            if index in all_indices:
                found_count += 1
                self.loggit.info('Found restored index %s', index)
            else:
                missing.append(index)
        if found_count == len(self.expected_output):
            self.loggit.info('All indices appear to have been restored.')
        else:
            msg = f'Some of the indices do not appear to have been restored. Missing: {missing}'
            self.loggit.error(msg)
            raise FailedRestore(msg)

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        args = {'wait_for_completion' : self.wfc, 'body' : self.body}
        msg = (
            f'DRY-RUN: restore: Repository: {self.repository} '
            f'Snapshot name: {self.name} Arguments: {args}'
        )
        self.loggit.info(msg)

        for index in self.indices:
            if self.rename_pattern and self.rename_replacement:
                rmsg = f'as {re.sub(self.rename_pattern, self.py_rename_replacement, index)}'
            else:
                rmsg = ''
            self.loggit.info('DRY-RUN: restore: Index %s %s', index, rmsg)

    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.SnapshotClient.restore` :py:attr:`indices` from
        :py:attr:`name` with passed params.
        """
        if not self.skip_repo_fs_check:
            verify_repository(self.client, self.repository)
        if snapshot_running(self.client):
            raise SnapshotInProgress('Cannot restore while a snapshot is in progress.')
        try:
            self.loggit.info('Restoring indices "%s" from snapshot: %s', self.indices, self.name)
            # Always set wait_for_completion to False. Let 'wait_for_it' do its
            # thing if wait_for_completion is set to True. Report the task_id
            # either way.
            self.client.snapshot.restore(
                repository=self.repository,
                snapshot=self.name,
                ignore_index_settings=None,
                ignore_unavailable=self.ignore_unavailable,
                include_aliases=self.include_aliases,
                include_global_state=self.include_global_state,
                index_settings=self.index_settings,
                indices=self.indices,
                partial=self.partial,
                rename_pattern=self.rename_pattern,
                rename_replacement=self.rename_replacement,
                wait_for_completion=False
            )
            if self.wfc:
                wait_for_it(
                    self.client, 'restore', index_list=self.expected_output,
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
                self.report_state()
            else:
                msg = (
                    f'"wait_for_completion" set to {self.wfc}. '
                    f'Remember to check for successful completion manually.'
                )
                self.loggit.warning(msg)
        except Exception as err:
            report_failure(err)
