from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionDeleteIndices(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, curator.DeleteIndices, 'invalid')
    def test_init_raise_bad_master_timeout(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        self.assertRaises(TypeError, curator.DeleteIndices, ilo, 'invalid')
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        do = curator.DeleteIndices(ilo)
        self.assertEqual(ilo, do.index_list)
        self.assertEqual(client, do.client)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = curator.IndexList(client)
        do = curator.DeleteIndices(ilo)
        self.assertIsNone(do.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = curator.IndexList(client)
        do = curator.DeleteIndices(ilo)
        self.assertIsNone(do.do_action())
    def test_do_action_not_successful(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = curator.IndexList(client)
        do = curator.DeleteIndices(ilo)
        self.assertIsNone(do.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        client.indices.delete.side_effect = testvars.fake_fail
        ilo = curator.IndexList(client)
        do = curator.DeleteIndices(ilo)
        self.assertRaises(curator.FailedExecution, do.do_action)
    def test_verify_result_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.delete.return_value = None
        ilo = curator.IndexList(client)
        do = curator.DeleteIndices(ilo)
        self.assertTrue(do._verify_result([],2))
