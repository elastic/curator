from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionOpen(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, curator.Open, 'invalid')
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        oo = curator.Open(ilo)
        self.assertEqual(ilo, oo.index_list)
        self.assertEqual(client, oo.client)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.open.return_value = None
        ilo = curator.IndexList(client)
        ilo.filter_opened()
        oo = curator.Open(ilo)
        self.assertEqual([u'c-2016.03.05'], oo.index_list.indices)
        self.assertIsNone(oo.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.open.return_value = None
        ilo = curator.IndexList(client)
        ilo.filter_opened()
        oo = curator.Open(ilo)
        self.assertEqual([u'c-2016.03.05'], oo.index_list.indices)
        self.assertIsNone(oo.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.indices.open.return_value = None
        client.indices.open.side_effect = testvars.fake_fail
        ilo = curator.IndexList(client)
        oo = curator.Open(ilo)
        self.assertRaises(curator.FailedExecution, oo.do_action)
