"""test_action_snapshot"""
from unittest import TestCase
from mock import Mock
from curator.actions import Snapshot
from curator.exceptions import ActionError, CuratorException, FailedExecution, FailedSnapshot, MissingArgument, SnapshotInProgress
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionSnapshot(TestCase):
    def test_init_raise_bad_index_list(self):
        self.assertRaises(TypeError, Snapshot, 'invalid')
    def test_init_no_repo_arg_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        self.assertRaises(MissingArgument, Snapshot, ilo)
    def test_init_no_repo_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        ilo = IndexList(client)
        self.assertRaises(
            ActionError, Snapshot, ilo, repository='notfound')
    def test_init_no_name_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        ilo = IndexList(client)
        self.assertRaises(MissingArgument, Snapshot, ilo,
            repository=testvars.repo_name)
    def test_init_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertEqual(testvars.repo_name, so.repository)
        self.assertIsNone(so.state)
    def test_get_state_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        so.get_state()
        self.assertEqual('SUCCESS', so.state)
    def test_get_state_fail(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = {'snapshots':[]}
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(CuratorException, so.get_state)
    def test_report_state_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        so.report_state()
        self.assertEqual('SUCCESS', so.state)
    def test_report_state_other(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.highly_unlikely
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(FailedSnapshot, so.report_state)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = None
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertIsNone(so.do_dry_run())
    def test_do_action_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertIsNone(so.do_action())
    def test_do_action_raise_snap_in_progress(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = None
        client.snapshot.status.return_value = testvars.snap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(SnapshotInProgress, so.do_action)
    def test_do_action_no_wait_for_completion(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = testvars.generic_task
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name, wait_for_completion=False)
        self.assertIsNone(so.do_action())
    def test_do_action_raise_on_failure(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = None
        client.snapshot.create.side_effect = testvars.fake_fail
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = IndexList(client)
        so = Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(FailedExecution, so.do_action)
