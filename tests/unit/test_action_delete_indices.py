"""test_action_delete_indices"""
from unittest import TestCase
from mock import Mock
from curator.actions import DeleteIndices
from curator.exceptions import FailedExecution
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionDeleteIndices(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, DeleteIndices, 'invalid')
    def test_init_raise_bad_master_timeout(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        self.assertRaises(TypeError, DeleteIndices, ilo, 'invalid')
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        do = DeleteIndices(ilo)
        self.assertEqual(ilo, do.index_list)
        self.assertEqual(client, do.client)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = IndexList(client)
        do = DeleteIndices(ilo)
        self.assertIsNone(do.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = IndexList(client)
        do = DeleteIndices(ilo)
        self.assertIsNone(do.do_action())
    def test_do_action_not_successful(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = IndexList(client)
        do = DeleteIndices(ilo)
        self.assertIsNone(do.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        client.indices.delete.side_effect = testvars.fake_fail
        ilo = IndexList(client)
        do = DeleteIndices(ilo)
        self.assertRaises(FailedExecution, do.do_action)
    def test_verify_result_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = IndexList(client)
        do = DeleteIndices(ilo)
        self.assertTrue(do._verify_result([],2))
