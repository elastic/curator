"""test_action_snapshot"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long, protected-access, attribute-defined-outside-init
from unittest import TestCase
from mock import Mock
from curator.actions import Snapshot
from curator.exceptions import ActionError, CuratorException, FailedExecution, FailedSnapshot, MissingArgument, SnapshotInProgress
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionSnapshot(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.client.snapshot.get_repository.return_value = testvars.test_repo
        self.client.snapshot.get.return_value = testvars.snapshots
        self.client.tasks.get.return_value = testvars.no_snap_tasks
        self.ilo = IndexList(self.client)
    def test_init_raise_bad_index_list(self):
        self.assertRaises(TypeError, Snapshot, 'invalid')
    def test_init_no_repo_arg_exception(self):
        self.builder()
        self.assertRaises(MissingArgument, Snapshot, self.ilo)
    def test_init_no_repo_exception(self):
        self.builder()
        self.client.snapshot.get_repository.return_value = {'repo':{'foo':'bar'}}
        self.assertRaises(ActionError, Snapshot, self.ilo, repository='notfound')
    def test_init_no_name_exception(self):
        self.builder()
        self.assertRaises(MissingArgument, Snapshot, self.ilo, repository=testvars.repo_name)
    def test_init_success(self):
        self.builder()
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        self.assertEqual(testvars.repo_name, sso.repository)
        self.assertIsNone(sso.state)
    def test_get_state_success(self):
        self.builder()
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        sso.get_state()
        self.assertEqual('SUCCESS', sso.state)
    def test_get_state_fail(self):
        self.builder()
        self.client.snapshot.get.return_value = {'snapshots':[]}
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        self.assertRaises(CuratorException, sso.get_state)
    def test_report_state_success(self):
        self.builder()
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        sso.report_state()
        self.assertEqual('SUCCESS', sso.state)
    def test_report_state_other(self):
        self.builder()
        self.client.snapshot.get.return_value = testvars.highly_unlikely
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        self.assertRaises(FailedSnapshot, sso.report_state)
    def test_do_dry_run(self):
        self.builder()
        self.client.snapshot.create.return_value = None
        self.client.snapshot.status.return_value = testvars.nosnap_running
        self.client.snapshot.verify_repository.return_value = testvars.verified_nodes
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        self.assertIsNone(sso.do_dry_run())
    def test_do_action_success(self):
        self.builder()
        self.client.snapshot.create.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task
        self.client.snapshot.status.return_value = testvars.nosnap_running
        self.client.snapshot.verify_repository.return_value = testvars.verified_nodes
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        self.assertIsNone(sso.do_action())
    def test_do_action_raise_snap_in_progress(self):
        self.builder()
        self.client.snapshot.create.return_value = None
        self.client.snapshot.status.return_value = testvars.snap_running
        self.client.snapshot.verify_repository.return_value = testvars.verified_nodes
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        self.assertRaises(SnapshotInProgress, sso.do_action)
    def test_do_action_no_wait_for_completion(self):
        self.builder()
        self.client.snapshot.create.return_value = testvars.generic_task
        self.client.snapshot.status.return_value = testvars.nosnap_running
        self.client.snapshot.verify_repository.return_value = testvars.verified_nodes
        sso = Snapshot(self.ilo, repository=testvars.repo_name,
            name=testvars.snap_name, wait_for_completion=False)
        self.assertIsNone(sso.do_action())
    def test_do_action_raise_on_failure(self):
        self.builder()
        self.client.snapshot.create.return_value = None
        self.client.snapshot.create.side_effect = testvars.fake_fail
        self.client.snapshot.status.return_value = testvars.nosnap_running
        self.client.snapshot.verify_repository.return_value = testvars.verified_nodes
        sso = Snapshot(self.ilo, repository=testvars.repo_name, name=testvars.snap_name)
        self.assertRaises(FailedExecution, sso.do_action)
