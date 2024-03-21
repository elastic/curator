"""test_action_restore"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock
from curator.actions import Restore
from curator.exceptions import CuratorException, FailedExecution, FailedRestore, SnapshotInProgress
from curator import SnapshotList
# Get test variables and constants from a single source
from . import testvars

class TestActionRestore(TestCase):
    def test_init_raise_bad_snapshot_list(self):
        self.assertRaises(TypeError, Restore, 'invalid')
    def test_init_raise_unsuccessful_snapshot_list(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.partial
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        self.assertRaises(CuratorException, Restore, slo)
    def test_snapshot_derived_name(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo)
        self.assertEqual('snapshot-2015.03.01', ro.name)
    def test_provided_name(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo, name=testvars.snap_name)
        self.assertEqual(testvars.snap_name, ro.name)
    def test_partial_snap(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.partial
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo, partial=True)
        self.assertEqual(testvars.snap_name, ro.name)
    def test_provided_indices(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo, indices=testvars.named_indices)
        self.assertEqual('snapshot-2015.03.01', ro.name)
    def test_extra_settings(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo, extra_settings={'foo':'bar'})
        self.assertEqual(ro.body['foo'], 'bar')
    def test_bad_extra_settings(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo, extra_settings='invalid')
        self.assertEqual(ro.body,
            {
                'ignore_unavailable': False,
                'partial': False,
                'include_aliases': False,
                'rename_replacement': '',
                'rename_pattern': '',
                'indices': ['index-2015.01.01', 'index-2015.02.01'],
                'include_global_state': False
            }
        )
    def test_get_expected_output(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(
            slo, rename_pattern='(.+)', rename_replacement='new_$1')
        self.assertEqual(
            ro.expected_output,
            ['new_index-2015.01.01', 'new_index-2015.02.01']
        )
    def test_do_dry_run(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo)
        self.assertIsNone(ro.do_dry_run())
    def test_do_dry_run_with_renames(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(
            slo, rename_pattern='(.+)', rename_replacement='new_$1')
        self.assertIsNone(ro.do_dry_run())
    def test_report_state_all(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.snapshot.get.return_value = testvars.snapshot
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.indices.get_settings.return_value = testvars.settings_named
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo)
        self.assertIsNone(ro.report_state())
    def test_report_state_not_all(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.indices.get_settings.return_value = testvars.settings_one
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(
            slo, rename_pattern='(.+)', rename_replacement='new_$1')
        self.assertRaises(FailedRestore, ro.report_state)
    def test_do_action_success(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        client.indices.get_settings.return_value = testvars.settings_named
        client.indices.recovery.return_value = testvars.recovery_output
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo, wait_interval=0.5, max_wait=1)
        self.assertIsNone(ro.do_action())
    def test_do_action_snap_in_progress(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.status.return_value = testvars.snap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        client.indices.get_settings.return_value = testvars.settings_named
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo)
        self.assertRaises(SnapshotInProgress, ro.do_action)
    def test_do_action_success_no_wfc(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        client.indices.get_settings.return_value = testvars.settings_named
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo, wait_for_completion=False)
        self.assertIsNone(ro.do_action())
    def test_do_action_report_on_failure(self):
        client = Mock()
        client.snapshot.get.return_value = testvars.snapshots
        client.snapshot.get_repository.return_value = testvars.test_repo
        client.snapshot.status.return_value = testvars.nosnap_running
        client.snapshot.verify_repository.return_value = testvars.verified_nodes
        client.indices.get_settings.return_value = testvars.settings_named
        client.snapshot.restore.side_effect = testvars.fake_fail
        slo = SnapshotList(client, repository=testvars.repo_name)
        ro = Restore(slo)
        self.assertRaises(FailedExecution, ro.do_action)
