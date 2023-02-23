"""test_action_reindex"""
from unittest import TestCase
from mock import Mock
from curator.actions import Reindex
from curator.exceptions import ConfigurationError, CuratorException, FailedExecution, NoIndices
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionReindex(TestCase):
    def test_init_bad_ilo(self):
        self.assertRaises(TypeError, Reindex, 'foo', 'invalid')
    def test_init_raise_bad_request_body(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        self.assertRaises(ConfigurationError,
            Reindex, ilo, 'invalid')
    def test_init_raise_local_migration_no_prefix_or_suffix(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        self.assertRaises(ConfigurationError,
            Reindex, ilo, testvars.reindex_migration)
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        ro = Reindex(ilo, testvars.reindex_basic)
        self.assertEqual(ilo, ro.index_list)
        self.assertEqual(client, ro.client)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        ilo = IndexList(client)
        ro = Reindex(ilo, testvars.reindex_basic)
        self.assertIsNone(ro.do_dry_run())
    def test_replace_index_list(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        ilo = IndexList(client)
        ro = Reindex(ilo, testvars.reindex_replace)
        self.assertEqual(ro.index_list.indices, ro.body['source']['index'])
    def test_reindex_with_wait(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task
        ilo = IndexList(client)
        # After building ilo, we need a different return value
        client.indices.get_settings.return_value = {'other_index':{}}
        ro = Reindex(ilo, testvars.reindex_basic)
        self.assertIsNone(ro.do_action())
    def test_reindex_with_wait_zero_total(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task_zero_total
        ilo = IndexList(client)
        # After building ilo, we need a different return value
        client.indices.get_settings.return_value = {'other_index':{}}
        ro = Reindex(ilo, testvars.reindex_basic)
        self.assertIsNone(ro.do_action())
    def test_reindex_with_wait_zero_total_fail(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.side_effect = testvars.fake_fail
        ilo = IndexList(client)
        # After building ilo, we need a different return value
        client.indices.get_settings.return_value = {'other_index':{}}
        ro = Reindex(ilo, testvars.reindex_basic)
        self.assertRaises(CuratorException, ro.do_action)
    def test_reindex_without_wait(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task
        ilo = IndexList(client)
        ro = Reindex(ilo, testvars.reindex_basic,
            wait_for_completion=False)
        self.assertIsNone(ro.do_action())
    def test_reindex_timedout(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.incomplete_task
        ilo = IndexList(client)
        ro = Reindex(ilo, testvars.reindex_basic,
             max_wait=1, wait_interval=1)
        self.assertRaises(FailedExecution, ro.do_action)
    def test_remote_with_no_host_key(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task
        ilo = IndexList(client)
        # After building ilo, we need a different return value
        client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'irrelevant',
                'remote': {'wrong': 'invalid'}
            },
            'dest': { 'index': 'other_index' }
        }
        self.assertRaises(
            ConfigurationError, Reindex, ilo, badval)
    def test_remote_with_bad_host(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task
        ilo = IndexList(client)
        # After building ilo, we need a different return value
        client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'irrelevant',
                'remote': {'host': 'invalid'}
            },
            'dest': { 'index': 'other_index' }
        }
        self.assertRaises(
            ConfigurationError, Reindex, ilo, badval)
    def test_remote_with_bad_url(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task
        ilo = IndexList(client)
        # After building ilo, we need a different return value
        client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'irrelevant',
                'remote': {'host': 'asdf://hostname:1234'}
            },
            'dest': { 'index': 'other_index' }
        }
        self.assertRaises(
            ConfigurationError, Reindex, ilo, badval)
    def test_remote_with_bad_connection(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_four
        client.cluster.state.return_value = testvars.clu_state_four
        client.indices.stats.return_value = testvars.stats_four
        client.reindex.return_value = testvars.generic_task
        client.tasks.get.return_value = testvars.completed_task
        ilo = IndexList(client)
        # After building ilo, we need a different return value
        client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'REINDEX_SELECTION',
                'remote': {'host': 'https://example.org:XXXX'}
            },
            'dest': { 'index': 'other_index' }
        }
        urllib3 = Mock()
        urllib3.util.retry.side_effect = testvars.fake_fail
        self.assertRaises(Exception, Reindex, ilo, badval)
    def test_init_raise_empty_source_list(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        badval = {
            'source': { 'index': [] },
            'dest': { 'index': 'other_index' }
        }
        ro = Reindex(ilo, badval)
        self.assertRaises(NoIndices, ro.do_action)
