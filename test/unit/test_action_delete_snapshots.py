from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionDeleteSnapshots(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, curator.DeleteSnapshots, 'invalid')
    def test_init(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = curator.SnapshotList(client, repository=testvars.repo_name)
        do = curator.DeleteSnapshots(slo)
        self.assertEqual(slo, do.snapshot_list)
        self.assertEqual(client, do.client)
    def test_do_dry_run(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.delete.return_value = None
        slo = curator.SnapshotList(client, repository=testvars.repo_name)
        do = curator.DeleteSnapshots(slo)
        self.assertIsNone(do.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.delete.return_value = None
        slo = curator.SnapshotList(client, repository=testvars.repo_name)
        do = curator.DeleteSnapshots(slo)
        self.assertIsNone(do.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.delete.return_value = None
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.delete.side_effect = testvars.fake_fail
        slo = curator.SnapshotList(client, repository=testvars.repo_name)
        do = curator.DeleteSnapshots(slo)
        self.assertRaises(curator.FailedExecution, do.do_action)
    def test_not_safe_to_snap_raises_exception(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.inprogress
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.tasks.get.return_value = testvars.no_snap_tasks
        slo = curator.SnapshotList(client, repository=testvars.repo_name)
        do = curator.DeleteSnapshots(slo, retry_interval=0, retry_count=1)
        self.assertRaises(curator.FailedExecution, do.do_action)
