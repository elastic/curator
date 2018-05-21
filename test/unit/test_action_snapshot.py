from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionSnapshot(TestCase):
    def test_init_raise_bad_index_list(self):
        self.assertRaises(TypeError, curator.Snapshot, 'invalid')
    def test_init_no_repo_arg_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        self.assertRaises(curator.MissingArgument, curator.Snapshot, ilo)
    def test_init_no_repo_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        ilo = curator.IndexList(client)
        self.assertRaises(
            curator.ActionError, curator.Snapshot, ilo, repository='notfound')
    def test_init_no_name_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        ilo = curator.IndexList(client)
        self.assertRaises(curator.MissingArgument, curator.Snapshot, ilo,
            repository=testvars.repo_name)
    def test_init_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertEqual(testvars.repo_name, so.repository)
        self.assertIsNone(so.state)
    def test_get_state_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        so.get_state()
        self.assertEqual('SUCCESS', so.state)
    def test_get_state_fail(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = {'snapshots':[]}
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(curator.CuratorException, so.get_state)
    def test_report_state_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        so.report_state()
        self.assertEqual('SUCCESS', so.state)
    def test_report_state_other(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.highly_unlikely
        client.tasks.get.return_value = testvars.no_snap_tasks
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(curator.exceptions.FailedSnapshot, so.report_state)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = None
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertIsNone(so.do_dry_run())
    def test_do_action_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
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
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertIsNone(so.do_action())
    def test_do_action_raise_snap_in_progress(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = None
        client.snapshot.status.return_value = testvars.snap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(curator.SnapshotInProgress, so.do_action)
    def test_do_action_no_wait_for_completion(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.get.return_value = testvars.snapshots
        client.tasks.get.return_value = testvars.no_snap_tasks
        client.snapshot.create.return_value = testvars.generic_task
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name, wait_for_completion=False)
        self.assertIsNone(so.do_action())
    def test_do_action_raise_on_failure(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
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
        ilo = curator.IndexList(client)
        so = curator.Snapshot(ilo, repository=testvars.repo_name,
            name=testvars.snap_name)
        self.assertRaises(curator.FailedExecution, so.do_action)
