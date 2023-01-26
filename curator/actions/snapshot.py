"""Snapshot and Restore action classes"""
import logging
import re
from es_client.helpers.utils import ensure_list
# pylint: disable=import-error, broad-except
from curator.exceptions import (
        ActionError, CuratorException, FailedExecution, FailedRestore, FailedSnapshot,
        MissingArgument, SnapshotInProgress
    )
from curator.utils import (
    get_indices, parse_datemath, parse_date_pattern, report_failure, repository_exists,
    safe_to_snap, snapshot_running, to_csv, test_repo_fs, verify_index_list,
    verify_snapshot_list, wait_for_it
    )

class Snapshot(object):
    """Snapshot Action Class"""
    def __init__(
            self, ilo, repository=None, name=None, ignore_unavailable=False,
            include_global_state=True, partial=False, wait_for_completion=True, wait_interval=9,
            max_wait=-1, skip_repo_fs_check=False
    ):
        """
        :arg ilo: A :class:`curator.indexlist.IndexList` object
        :arg repository: The Elasticsearch snapshot repository to use
        :arg name: What to name the snapshot.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :type wait_for_completion: bool
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :arg ignore_unavailable: Ignore unavailable shards/indices.
            (default: `False`)
        :type ignore_unavailable: bool
        :arg include_global_state: Store cluster global state with snapshot.
            (default: `True`)
        :type include_global_state: bool
        :arg partial: Do not fail if primary shard is unavailable. (default:
            `False`)
        :type partial: bool
        :arg skip_repo_fs_check: Do not validate write access to repository on
            all cluster nodes before proceeding. (default: `False`).  Useful for
            shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success.
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
        #: Instance variable.
        #: The Elasticsearch Client object derived from `ilo`
        self.client = ilo.client
        #: Instance variable.
        #: The parsed version of `name`
        self.name = parse_datemath(self.client, parse_date_pattern(name))
        #: Instance variable.
        #: Internal reference to `ilo`
        self.index_list = ilo
        #: Instance variable.
        #: Internally accessible copy of `repository`
        self.repository = repository
        #: Instance variable.
        #: Internally accessible copy of `wait_for_completion`
        self.wait_for_completion = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait = max_wait
        #: Instance variable.
        #: Internally accessible copy of `skip_repo_fs_check`
        self.skip_repo_fs_check = skip_repo_fs_check
        self.state = None

        #: Instance variable.
        #: Populated at instance creation time by calling
        #: :mod:`curator.create_snapshot_body` with `ilo.indices` and the
        #: provided arguments: `ignore_unavailable`, `include_global_state`,
        #: `partial`
        # self.body = create_snapshot_body(
        #     ilo.indices,
        #     ignore_unavailable=ignore_unavailable,
        #     include_global_state=include_global_state,
        #     partial=partial
        # )
        self.indices = to_csv(ilo.indices)
        self.ignore_unavailable = ignore_unavailable
        self.include_global_state = include_global_state
        self.partial = partial
        self.settings = {
            'indices': ilo.indices,
            'ignore_unavailable': self.ignore_unavailable,
            'include_global_state': self.include_global_state,
            'partial': self.partial
        }

        self.loggit = logging.getLogger('curator.actions.snapshot')

    def get_state(self):
        """
        Get the state of the snapshot
        """
        try:
            self.state = self.client.snapshot.get(
                repository=self.repository,
                snapshot=self.name)['snapshots'][0]['state']
            return self.state
        except IndexError as exc:
            raise CuratorException(
                f'Snapshot "{self.name}" not found in repository "{self.repository}"') from exc

    def report_state(self):
        """
        Log the state of the snapshot and raise an exception if the state is
        not ``SUCCESS``
        """
        self.get_state()
        if self.state == 'SUCCESS':
            self.loggit.info('Snapshot %s successfully completed.', self.name)
        else:
            msg = f'Snapshot {self.name} completed with state: {self.state}'
            self.loggit.error(msg)
            raise FailedSnapshot(msg)

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        msg = (
            f'DRY-RUN: snapshot: {self.name} in repository {self.repository} '
            f'with arguments: {self.settings}'
        )
        self.loggit.info(msg)

    def do_action(self):
        """
        Snapshot indices in `index_list.indices`, with options passed.
        """
        if not self.skip_repo_fs_check:
            test_repo_fs(self.client, self.repository)
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
        :arg slo: A :class:`curator.snapshotlist.SnapshotList` object
        :arg retry_interval: Number of seconds to delay betwen retries. Default:
            120 (seconds)
        :arg retry_count: Number of attempts to make. Default: 3
        """
        verify_snapshot_list(slo)
        #: Instance variable.
        #: The Elasticsearch Client object derived from `slo`
        self.client = slo.client
        #: Instance variable.
        #: Internally accessible copy of `retry_interval`
        self.retry_interval = retry_interval
        #: Instance variable.
        #: Internally accessible copy of `retry_count`
        self.retry_count = retry_count
        #: Instance variable.
        #: Internal reference to `slo`
        self.snapshot_list = slo
        #: Instance variable.
        #: The repository name derived from `slo`
        self.repository = slo.repository
        self.loggit = logging.getLogger('curator.actions.delete_snapshots')

    def do_dry_run(self):
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        mykwargs = {
            'repository' : self.repository,
            'retry_interval' : self.retry_interval,
            'retry_count' : self.retry_count,
        }
        for snap in self.snapshot_list.snapshots:
            self.loggit.info(
                'DRY-RUN: delete_snapshot: %s with arguments: %s', snap, mykwargs)

    def do_action(self):
        """
        Delete snapshots in `slo`
        Retry up to `retry_count` times, pausing `retry_interval`
        seconds between retries.
        """
        self.snapshot_list.empty_list_check()
        msg = (
            f'Deleting {len(self.snapshot_list.snapshots)} '
            f'selected snapshots: {self.snapshot_list.snapshots}'
        )
        self.loggit.info(msg)
        if not safe_to_snap(
                self.client, repository=self.repository,
                retry_interval=self.retry_interval, retry_count=self.retry_count
        ):
            raise FailedExecution(
                'Unable to delete snapshot(s) because a snapshot is in '
                'state "IN_PROGRESS"')
        try:
            for snap in self.snapshot_list.snapshots:
                self.loggit.info('Deleting snapshot %s...', snap)
                self.client.snapshot.delete(repository=self.repository, snapshot=snap)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)

class Restore(object):
    """Restore Action Class"""
    def __init__(
            self, slo, name=None, indices=None, include_aliases=False, ignore_unavailable=False,
            include_global_state=False, partial=False, rename_pattern=None,
            rename_replacement=None, extra_settings=None, wait_for_completion=True, wait_interval=9,
            max_wait=-1, skip_repo_fs_check=False
    ):
        """
        :arg slo: A :class:`curator.snapshotlist.SnapshotList` object
        :arg name: Name of the snapshot to restore.  If no name is provided, it
            will restore the most recent snapshot by age.
        :type name: str
        :arg indices: A list of indices to restore.  If no indices are provided,
            it will restore all indices in the snapshot.
        :type indices: list
        :arg include_aliases: If set to `True`, restore aliases with the
            indices. (default: `False`)
        :type include_aliases: bool
        :arg ignore_unavailable: Ignore unavailable shards/indices.
            (default: `False`)
        :type ignore_unavailable: bool
        :arg include_global_state: Restore cluster global state with snapshot.
            (default: `False`)
        :type include_global_state: bool
        :arg partial: Do not fail if primary shard is unavailable. (default:
            `False`)
        :type partial: bool
        :arg rename_pattern: A regular expression pattern with one or more
            captures, e.g. ``index_(.+)``
        :type rename_pattern: str
        :arg rename_replacement: A target index name pattern with `$#` numbered
            references to the captures in ``rename_pattern``, e.g.
            ``restored_index_$1``
        :type rename_replacement: str
        :arg extra_settings: Extra settings, including shard count and settings
            to omit. For more information see
            https://www.elastic.co/guide/en/elasticsearch/reference/8.6/snapshots-restore-snapshot.html#change-index-settings-during-restore
        :type extra_settings: dict, representing the settings.
        :arg wait_for_completion: Wait (or not) for the operation
            to complete before returning.  (default: `True`)
        :arg wait_interval: How long in seconds to wait between checks for
            completion.
        :arg max_wait: Maximum number of seconds to `wait_for_completion`
        :type wait_for_completion: bool

        :arg skip_repo_fs_check: Do not validate write access to repository on
            all cluster nodes before proceeding. (default: `False`).  Useful for
            shared filesystems where intermittent timeouts can affect
            validation, but won't likely affect snapshot success.
        :type skip_repo_fs_check: bool
        """
        if extra_settings is None:
            extra_settings = {}
        self.loggit = logging.getLogger('curator.actions.snapshot')
        verify_snapshot_list(slo)
        # Get the most recent snapshot.
        most_recent = slo.most_recent()
        self.loggit.debug('"most_recent" snapshot: %s', most_recent)
        #: Instance variable.
        #: Will use a provided snapshot name, or the most recent snapshot in slo
        self.name = name if name else most_recent
        # Stop here now, if it's not a successful snapshot.
        if slo.snapshot_info[self.name]['state'] == 'PARTIAL' and partial:
            self.loggit.warning('Performing restore of snapshot in state PARTIAL.')
        elif slo.snapshot_info[self.name]['state'] != 'SUCCESS':
            raise CuratorException(
                'Restore operation can only be performed on snapshots with '
                'state "SUCCESS", or "PARTIAL" if partial=True.'
            )
        #: Instance variable.
        #: The Elasticsearch Client object derived from `slo`
        self.client = slo.client
        #: Instance variable.
        #: Internal reference to `slo`
        self.snapshot_list = slo
        #: Instance variable.
        #: `repository` derived from `slo`
        self.repository = slo.repository

        if indices:
            self.indices = ensure_list(indices)
        else:
            self.indices = slo.snapshot_info[self.name]['indices']
        self.wfc = wait_for_completion
        #: Instance variable
        #: How many seconds to wait between checks for completion.
        self.wait_interval = wait_interval
        #: Instance variable.
        #: How long in seconds to `wait_for_completion` before returning with an
        #: exception. A value of -1 means wait forever.
        self.max_wait = max_wait
        #: Instance variable version of ``rename_pattern``
        self.rename_pattern = rename_pattern if rename_replacement is not None \
            else ''
        #: Instance variable version of ``rename_replacement``
        self.rename_replacement = rename_replacement if rename_replacement \
            is not None else ''
        #: Also an instance variable version of ``rename_replacement``
        #: but with Java regex group designations of ``$#``
        #: converted to Python's ``\\#`` style.
        self.py_rename_replacement = self.rename_replacement.replace('$', '\\')
        #: Instance variable.
        #: Internally accessible copy of `skip_repo_fs_check`
        self.skip_repo_fs_check = skip_repo_fs_check

        #: Instance variable.
        #: Populated at instance creation time from the other options
        self.body = {
            'indices' : self.indices,
            'include_aliases' : include_aliases,
            'ignore_unavailable' : ignore_unavailable,
            'include_global_state' : include_global_state,
            'partial' : partial,
            'rename_pattern' : self.rename_pattern,
            'rename_replacement' : self.rename_replacement,
        }
        self.include_aliases = include_aliases
        self.ignore_unavailable = ignore_unavailable
        self.include_global_state = include_global_state
        self.include_aliases = include_aliases
        self.partial = partial
        self.rename_pattern = rename_pattern
        self.rename_replacement = rename_replacement
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
        Log the state of the restore
        This should only be done if ``wait_for_completion`` is `True`, and only
        after completing the restore.
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
        """
        Log what the output would be, but take no action.
        """
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        args = {'wait_for_completion' : self.wfc, 'body' : self.body}
        msg = (
            f'DRY-RUN: restore: Repository: {self.repository} '
            f'Snapshot name: {self.name} Arguments: {args}'
        )
        self.loggit.info(msg)

        for index in self.indices:
            if self.rename_pattern and self.rename_replacement:
                replacement_msg = 'as {0}'.format(
                    re.sub(
                        self.rename_pattern,
                        self.py_rename_replacement,
                        index
                    )
                )
            else:
                replacement_msg = ''
            self.loggit.info(
                'DRY-RUN: restore: Index %s %s', index, replacement_msg
            )

    def do_action(self):
        """
        Restore indices with options passed.
        """
        if not self.skip_repo_fs_check:
            test_repo_fs(self.client, self.repository)
        if snapshot_running(self.client):
            raise SnapshotInProgress('Cannot restore while a snapshot is in progress.')
        try:
            self.loggit.info(
                'Restoring indices "%s" from snapshot: %s', self.indices, self.name
            )
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
