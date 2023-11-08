"""test_action_indexsettings"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock
from curator.actions import IndexSettings
from curator.exceptions import ActionError, ConfigurationError, MissingArgument
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionIndexSettings(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_init_raise_bad_index_list(self):
        self.assertRaises(TypeError, IndexSettings, 'invalid')
    def test_init_no_index_settings(self):
        self.builder()
        _ = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertRaises(MissingArgument, IndexSettings, self.ilo, {})
    def test_init_bad_index_settings(self):
        self.builder()
        _ = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertRaises(ConfigurationError, IndexSettings, self.ilo, {'a':'b'})
    def test_init(self):
        self.builder()
        iso = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertEqual(self.ilo, iso.index_list)
        self.assertEqual(self.client, iso.client)
    def test_static_settings(self):
        static = [
            'number_of_shards',
            'shard',
            'codec',
            'routing_partition_size',
        ]
        self.builder()
        iso = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertEqual(static, iso._static_settings())
    def test_dynamic_settings(self):
        self.builder()
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
        iso = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertEqual(dynamic, iso._dynamic_settings())
    def test_settings_check_raises_with_opened(self):
        self.builder()
        self.ilo.get_index_state()
        self.ilo.get_index_settings()
        iso = IndexSettings(self.ilo, {'index':{'codec':'best_compression'}})
        self.assertRaises(ActionError, iso._settings_check)
    def test_settings_check_no_raise_with_ignore_unavailable(self):
        self.builder()
        iso = IndexSettings(
            self.ilo, {'index':{'codec':'best_compression'}}, ignore_unavailable=True
        )
        self.assertIsNone(iso._settings_check())
    def test_settings_check_no_raise_with_dynamic_settings(self):
        self.builder()
        iso = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertIsNone(iso._settings_check())
    def test_settings_check_no_raise_with_unknown(self):
        self.builder()
        iso = IndexSettings(self.ilo, {'index':{'foobar':'1s'}})
        self.assertIsNone(iso._settings_check())
    def test_settings_dry_run(self):
        self.builder()
        iso = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertIsNone(iso.do_dry_run())
    def test_settings_do_action(self):
        self.builder()
        self.client.indices.put_settings.return_value = {"acknowledged":True}
        iso = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertIsNone(iso.do_action())
    def test_settings_do_action_raises(self):
        self.builder()
        self.client.indices.put_settings.side_effect = testvars.fake_fail
        iso = IndexSettings(self.ilo, {'index':{'refresh_interval':'1s'}})
        self.assertRaises(Exception, iso.do_action)
