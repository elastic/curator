"""test_action_delete_snapshots"""
from unittest import TestCase
from mock import Mock
from curator.actions import DeleteSnapshots
from curator.exceptions import FailedExecution
from curator import SnapshotList
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionDeleteSnapshots(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, DeleteSnapshots, 'invalid')
    def test_init(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        do = DeleteSnapshots(slo)
        self.assertEqual(slo, do.snapshot_list)
        self.assertEqual(client, do.client)
    def test_do_dry_run(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.delete.return_value = None
        slo = SnapshotList(client, repository=testvars.repo_name)
        do = DeleteSnapshots(slo)
        self.assertIsNone(do.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.tasks.list.return_value = testvars.no_snap_tasks
        client.snapshot.delete.return_value = None
        slo = SnapshotList(client, repository=testvars.repo_name)
        do = DeleteSnapshots(slo)
        self.assertIsNone(do.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.delete.return_value = None
        client.tasks.list.return_value = testvars.no_snap_tasks
        client.snapshot.delete.side_effect = testvars.fake_fail
        slo = SnapshotList(client, repository=testvars.repo_name)
        do = DeleteSnapshots(slo)
        self.assertRaises(FailedExecution, do.do_action)
    ### This check is not necessary after ES 7.16 as it is possible to have
    ### up to 1000 concurrent snapshots
    ###
    ### https://www.elastic.co/guide/en/elasticsearch/reference/8.6/snapshot-settings.html
    ### snapshot.max_concurrent_operations
    ### (Dynamic, integer) Maximum number of concurrent snapshot operations. Defaults to 1000.
    ###
    ### This limit applies in total to all ongoing snapshot creation, cloning, and deletion
    ### operations. Elasticsearch will reject any operations that would exceed this limit.
    # def test_not_safe_to_snap_raises_exception(self):
    #     client = Mock()
    #     client.snapshot.get.return_value = testvars.inprogress
    #     client.snapshot.get_repository.return_value = testvars.test_repo
    #     client.tasks.list.return_value = testvars.no_snap_tasks
    #     slo = SnapshotList(client, repository=testvars.repo_name)
    #     do = DeleteSnapshots(slo, retry_interval=0, retry_count=1)
    #     self.assertRaises(curator.FailedExecution, do.do_action)
