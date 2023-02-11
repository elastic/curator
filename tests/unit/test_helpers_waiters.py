"""Unit tests for utils"""
from unittest import TestCase
import pytest
from mock import Mock
from curator.exceptions import ActionTimeout, ConfigurationError, CuratorException, MissingArgument
from curator.helpers.waiters import (
    health_check, restore_check, snapshot_check,task_check, wait_for_it)

FAKE_FAIL = Exception('Simulated Failure')

class TestHealthCheck(TestCase):
    """TestHealthCheck

    Test helpers.waiters.health_check functionality
    """
    # pylint: disable=line-too-long
    CLUSTER_HEALTH = {"cluster_name": "unit_test", "status": "green", "timed_out": False, "number_of_nodes": 7, "number_of_data_nodes": 3, "active_primary_shards": 235, "active_shards": 471, "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": 0, "delayed_unassigned_shards": 0, "number_of_pending_tasks": 0,  "task_max_waiting_in_queue_millis": 0, "active_shards_percent_as_number": 100}
    def test_no_kwargs(self):
        """test_no_kwargs

        Should raise a ``MissingArgument`` exception when no keyword args are passed.
        """
        client = Mock()
        with pytest.raises(MissingArgument, match=r'Must provide at least one keyword argument'):
            health_check(client)
    def test_key_value_match(self):
        """test_key_value_match

        Should return ``True`` when matching keyword args are passed.
        """
        client = Mock()
        client.cluster.health.return_value = self.CLUSTER_HEALTH
        assert health_check(client, status='green')
    def test_key_value_no_match(self):
        """test_key_value_no_match

        Should return ``False`` when matching keyword args are passed, but no matches are found.
        """
        client = Mock()
        client.cluster.health.return_value = self.CLUSTER_HEALTH
        assert not health_check(client, status='red')
    def test_key_not_found(self):
        """test_key_not_found

        Should raise ``ConfigurationError`` when keyword args are passed, but keys match.
        """
        client = Mock()
        client.cluster.health.return_value = self.CLUSTER_HEALTH
        with pytest.raises(ConfigurationError, match=r'not in cluster health output'):
            health_check(client, foo='bar')

class TestRestoreCheck(TestCase):
    """TestRestoreCheck

    Test helpers.waiters.restore_check functionality
    """
    SNAP_NAME = 'snap_name'
    NAMED_INDICES = [ "index-2015.01.01", "index-2015.02.01" ]
    def test_fail_to_get_recovery(self):
        """test_fail_to_get_recovery

        Should raise ``CuratorException`` when an upstream Exception is encountered
        """
        client = Mock()
        client.indices.recovery.side_effect = FAKE_FAIL
        with pytest.raises(CuratorException, match=r'Unable to obtain recovery information'):
            restore_check(client, [])
    def test_incomplete_recovery(self):
        """test_incomplete_recovery

        Should return ``False`` when recovery is incomplete
        """
        client = Mock()
        # :pylint disable=line-too-long
        client.indices.recovery.return_value = {'index-2015.01.01': {'shards' : [{'stage':'INDEX'}]}, 'index-2015.02.01': {'shards' : [{'stage':'INDEX'}]}}
        assert not restore_check(client, self.NAMED_INDICES)
    def test_completed_recovery(self):
        """test_completed_recovery

        Should return ``True`` when recovery is complete
        """
        client = Mock()
        # :pylint disable=line-too-long
        client.indices.recovery.return_value = {'index-2015.01.01': {'shards' : [{'stage':'DONE'}]}, 'index-2015.02.01': {'shards' : [{'stage':'DONE'}]}}
        assert restore_check(client, self.NAMED_INDICES)
    def test_empty_recovery(self):
        """test_empty_recovery

        Should return ``False`` when an empty response comes back
        """
        client = Mock()
        client.indices.recovery.return_value = {}
        assert not restore_check(client, self.NAMED_INDICES)

class TestSnapshotCheck(TestCase):
    """TestSnapshotCheck

    Test helpers.waiters.snapshot_check functionality
    """
    # :pylint disable=line-too-long
    SNAP_NAME = 'snap_name'
    NAMED_INDICES = [ "index-2015.01.01", "index-2015.02.01" ]
    def test_fail_to_get_snapshot(self):
        """test_fail_to_get_snapshot

        Should raise ``CuratorException`` when another upstream Exception occurs.
        """
        client = Mock()
        client.snapshot.get.side_effect = FAKE_FAIL
        self.assertRaises(CuratorException, snapshot_check, client)
    def test_in_progress(self):
        """test_in_progress

        Should return ``False`` when state is ``IN_PROGRESS``.
        """
        client = Mock()
        test_val = {'snapshots':
            [{'state': 'IN_PROGRESS', 'snapshot': self.SNAP_NAME, 'indices': self.NAMED_INDICES}]}
        client.snapshot.get.return_value = test_val
        assert not snapshot_check(client, repository='foo', snapshot=self.SNAP_NAME)
    def test_success(self):
        """test_success

        Should return ``True`` when state is ``SUCCESS``.
        """
        client = Mock()
        test_val = {'snapshots':
            [{'state': 'SUCCESS', 'snapshot': self.SNAP_NAME, 'indices': self.NAMED_INDICES}]}
        client.snapshot.get.return_value = test_val
        assert snapshot_check(client, repository='foo', snapshot=self.SNAP_NAME)
    def test_partial(self):
        """test_partial

        Should return ``True`` when state is ``PARTIAL``.
        """
        client = Mock()
        test_val = {'snapshots':
            [{'state': 'PARTIAL', 'snapshot': self.SNAP_NAME, 'indices': self.NAMED_INDICES}]}
        client.snapshot.get.return_value = test_val
        assert snapshot_check(client, repository='foo', snapshot=self.SNAP_NAME)
    def test_failed(self):
        """test_failed

        Should return ``True`` when state is ``FAILED``.
        """
        client = Mock()
        test_val = {'snapshots':
            [{'state': 'FAILED', 'snapshot': self.SNAP_NAME, 'indices': self.NAMED_INDICES}]}
        client.snapshot.get.return_value = test_val
        assert snapshot_check(client, repository='foo', snapshot=self.SNAP_NAME)
    def test_other(self):
        """test_other

        Should return ``True`` when state is anything other than ``IN_PROGRESS`` or the above.
        """
        client = Mock()
        test_val = {'snapshots':
            [{'state': 'SOMETHINGELSE', 'snapshot': self.SNAP_NAME, 'indices': self.NAMED_INDICES}]}
        client.snapshot.get.return_value = test_val
        assert snapshot_check(client, repository='foo', snapshot=self.SNAP_NAME)

class TestTaskCheck(TestCase):
    """TestTaskCheck

    Test helpers.waiters.task_check functionality
    """
    # pylint: disable=line-too-long
    PROTO_TASK = {'node': 'I0ekFjMhSPCQz7FUs1zJOg', 'description': 'UNIT TEST', 'running_time_in_nanos': 1637039537721, 'action': 'indices:data/write/reindex', 'id': 54510686, 'start_time_in_millis': 1489695981997}
    GENERIC_TASK = {'task': 'I0ekFjMhSPCQz7FUs1zJOg:54510686'}
    def test_bad_task_id(self):
        """test_bad_task_id

        Should raise ``CuratorException`` if a bad value for ``task_id`` is passed
        """
        client = Mock()
        client.tasks.get.side_effect = FAKE_FAIL
        with pytest.raises(CuratorException, match=r'Unable to obtain task information for task'):
            task_check(client, 'foo')
    def test_incomplete_task(self):
        """test_incomplete_task

        Should return ``False`` if task is incomplete
        """
        client = Mock()
        test_task = {'completed': False, 'task': self.PROTO_TASK, 'response': {'failures': []}}
        client.tasks.get.return_value = test_task
        assert not task_check(client, task_id=self.GENERIC_TASK['task'])
    def test_complete_task(self):
        """test_complete_task

        Should return ``True`` if task is complete
        """
        client = Mock()
        test_task = {'completed': True, 'task': self.PROTO_TASK, 'response': {'failures': []}}
        client.tasks.get.return_value = test_task
        assert task_check(client, task_id=self.GENERIC_TASK['task'])

class TestWaitForIt(TestCase):
    """TestWaitForIt

    Test helpers.waiters.wait_for_it functionality
    """
    # pylint: disable=line-too-long
    def test_bad_action(self):
        """test_bad_action

        Should raise a ``ConfigurationError`` exception if ``action`` is invalid
        """
        client = Mock()
        # self.assertRaises(ConfigurationError, wait_for_it, client, 'foo')
        with pytest.raises(ConfigurationError, match=r'"action" must be one of'):
            wait_for_it(client, 'foo')
    def test_reindex_action_no_task_id(self):
        """test_reindex_action_no_task_id

        Should raise a ``MissingArgument`` exception if ``task_id`` is missing for ``reindex``
        """
        client = Mock()
        # self.assertRaises(MissingArgument, wait_for_it, client, 'reindex')
        with pytest.raises(MissingArgument, match=r'A task_id must accompany "action"'):
            wait_for_it(client, 'reindex')
    def test_snapshot_action_no_snapshot(self):
        """test_snapshot_action_no_snapshot

        Should raise a ``MissingArgument`` exception if ``snapshot`` is missing for ``snapshot``
        """
        client = Mock()
        # self.assertRaises(MissingArgument, wait_for_it, client, 'snapshot', repository='foo')
        with pytest.raises(MissingArgument, match=r'A snapshot and repository must accompany "action"'):
            wait_for_it(client, 'snapshot', repository='foo')
    def test_snapshot_action_no_repository(self):
        """test_snapshot_action_no_repository

        Should raise a ``MissingArgument`` exception if ``repository`` is missing for ``snapshot``
        """
        client = Mock()
        # self.assertRaises(MissingArgument, wait_for_it, client, 'snapshot', snapshot='foo')
        with pytest.raises(MissingArgument, match=r'A snapshot and repository must accompany "action"'):
            wait_for_it(client, 'snapshot', snapshot='foo')
    def test_restore_action_no_indexlist(self):
        """test_restore_action_no_indexlist

        Should raise a ``MissingArgument`` exception if ``index_list`` is missing for ``restore``
        """
        client = Mock()
        # self.assertRaises(MissingArgument, wait_for_it, client, 'restore')
        with pytest.raises(MissingArgument, match=r'An index_list must accompany "action"'):
            wait_for_it(client, 'restore')
    def test_reindex_action_bad_task_id(self):
        """test_reindex_action_bad_task_id

        Should raise a ``CuratorException`` exception if there's a bad task_id

        This is kind of a fake fail, even in the code.
        """
        client = Mock()
        client.tasks.get.return_value = {'a':'b'}
        client.tasks.get.side_effect = FAKE_FAIL
        # self.assertRaises(CuratorException, wait_for_it, client, 'reindex', task_id='foo')
        with pytest.raises(CuratorException, match=r'Unable to find task_id'):
            wait_for_it(client, 'reindex', task_id='foo')
    def test_reached_max_wait(self):
        """test_reached_max_wait

        Should raise a ``ActionTimeout`` exception if we've waited past the defined timeout period
        """
        client = Mock()
        client.cluster.health.return_value = {'status':'red'}
        # self.assertRaises(ActionTimeout, wait_for_it, client, 'replicas', wait_interval=1, max_wait=1)
        with pytest.raises(ActionTimeout, match=r'failed to complete in the max_wait period'):
            wait_for_it(client, 'replicas', wait_interval=1, max_wait=1)
