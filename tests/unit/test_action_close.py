"""Alias unit tests"""
# pylint: disable=missing-function-docstring, missing-class-docstring, invalid-name, line-too-long
from unittest import TestCase
from mock import Mock
from curator import IndexList
from curator.exceptions import FailedExecution
from curator.actions.close import Close
from . import testvars

class TestActionClose(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, Close, 'invalid')
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        co = Close(ilo)
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
        ilo = IndexList(client)
        co = Close(ilo)
        self.assertIsNone(co.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = IndexList(client)
        co = Close(ilo)
        self.assertIsNone(co.do_action())
    def test_do_action_with_delete_aliases(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = IndexList(client)
        co = Close(ilo, delete_aliases=True)
        self.assertIsNone(co.do_action())
    def test_do_action_with_skip_flush(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = IndexList(client)
        co = Close(ilo, skip_flush=True)
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
        ilo = IndexList(client)
        co = Close(ilo)
        self.assertRaises(FailedExecution, co.do_action)
    def test_do_action_delete_aliases_with_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.flush_synced.return_value = testvars.synced_pass
        client.indices.close.return_value = None
        ilo = IndexList(client)
        client.indices.delete_alias.side_effect = testvars.fake_fail
        co = Close(ilo, delete_aliases=True)
        self.assertIsNone(co.do_action())
