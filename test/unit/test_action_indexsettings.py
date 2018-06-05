from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionIndexSettings(TestCase):
    def test_init_raise_bad_index_list(self):
        self.assertRaises(TypeError, curator.IndexSettings, 'invalid')
    def test_init_no_index_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertRaises(
            curator.MissingArgument, curator.IndexSettings, ilo, {}
        )
    def test_init_bad_index_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertRaises(
            curator.ConfigurationError, curator.IndexSettings, ilo, {'a':'b'}
        )
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertEqual(ilo, iso.index_list)
        self.assertEqual(client, iso.client)
    def test_static_settings(self):
        static = [
            'number_of_shards',
            'shard',
            'codec',
            'routing_partition_size',
        ]
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertEqual(static, iso._static_settings())
    def test_dynamic_settings(self):
        dynamic = [
            'number_of_replicas',
            'auto_expand_replicas',
            'refresh_interval',
            'max_result_window',
            'max_rescore_window',
            'blocks',
            'max_refresh_listeners',
            'mapping',
            'merge',
            'translog',
        ]
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertEqual(dynamic, iso._dynamic_settings())
    def test_settings_check_raises_with_opened(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'codec':'best_compression'}})
        self.assertRaises(curator.ActionError, iso._settings_check)
    def test_settings_check_no_raise_with_ignore_unavailable(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(
            ilo, {'index':{'codec':'best_compression'}}, ignore_unavailable=True
        )
        self.assertIsNone(iso._settings_check())
    def test_settings_check_no_raise_with_dynamic_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertIsNone(iso._settings_check())
    def test_settings_check_no_raise_with_unknown(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'foobar':'1s'}})
        self.assertIsNone(iso._settings_check())
    def test_settings_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertIsNone(iso.do_dry_run())
    def test_settings_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = {"acknowledged":True}
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertIsNone(iso.do_action())
    def test_settings_do_action_raises(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.side_effect = testvars.fake_fail
        ilo = curator.IndexList(client)
        iso = curator.IndexSettings(ilo, {'index':{'refresh_interval':'1s'}})
        self.assertRaises(Exception, iso.do_action)