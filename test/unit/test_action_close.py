from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionClose(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, curator.Close, 'invalid')
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        co = curator.Close(ilo)
        self.assertEqual(ilo, co.index_list)
        self.assertEqual(client, co.client)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = curator.IndexList(client)
        co = curator.Close(ilo)
        self.assertIsNone(co.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = curator.IndexList(client)
        co = curator.Close(ilo)
        self.assertIsNone(co.do_action())
    def test_do_action_with_delete_aliases(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = curator.IndexList(client)
        co = curator.Close(ilo, delete_aliases=True)
        self.assertIsNone(co.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        client.indices.close.side_effect = testvars.fake_fail
        ilo = curator.IndexList(client)
        co = curator.Close(ilo)
        self.assertRaises(curator.FailedExecution, co.do_action)
    def test_do_action_delete_aliases_with_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = curator.IndexList(client)
        client.indices.delete_alias.side_effect = testvars.fake_fail
        co = curator.Close(ilo, delete_aliases=True)
        self.assertIsNone(co.do_action())
