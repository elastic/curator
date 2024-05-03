"""test_action_reindex"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock
from curator.actions import Reindex
from curator.exceptions import ConfigurationError, CuratorException, FailedExecution, NoIndices
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionReindex(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_four
        self.client.indices.get_settings.return_value = testvars.settings_four
        self.client.indices.stats.return_value = testvars.stats_four
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_init_bad_ilo(self):
        self.assertRaises(TypeError, Reindex, 'foo', 'invalid')
    def test_init_raise_bad_request_body(self):
        self.builder()
        self.assertRaises(ConfigurationError, Reindex, self.ilo, 'invalid')
    def test_init_raise_local_migration_no_prefix_or_suffix(self):
        self.builder()
        self.assertRaises(ConfigurationError, Reindex, self.ilo, testvars.reindex_migration)
    def test_init(self):
        self.builder()
        rio = Reindex(self.ilo, testvars.reindex_basic)
        self.assertEqual(self.ilo, rio.index_list)
        self.assertEqual(self.client, rio.client)
    def test_do_dry_run(self):
        self.builder()
        rio = Reindex(self.ilo, testvars.reindex_basic)
        self.assertIsNone(rio.do_dry_run())
    def test_replace_index_list(self):
        self.builder()
        rio = Reindex(self.ilo, testvars.reindex_replace)
        self.assertEqual(rio.index_list.indices, rio.body['source']['index'])
    def test_reindex_with_wait(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task
        # After building ilo, we need a different return value
        self.client.indices.get_settings.return_value = {'other_index':{}}
        rio = Reindex(self.ilo, testvars.reindex_basic)
        self.assertIsNone(rio.do_action())
    def test_reindex_with_wait_zero_total(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task_zero_total
        # After building ilo, we need a different return value
        self.client.indices.get_settings.return_value = {'other_index':{}}
        rio = Reindex(self.ilo, testvars.reindex_basic)
        self.assertIsNone(rio.do_action())
    def test_reindex_with_wait_zero_total_fail(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.side_effect = testvars.fake_fail
        # After building ilo, we need a different return value
        self.client.indices.get_settings.return_value = {'other_index':{}}
        rio = Reindex(self.ilo, testvars.reindex_basic)
        self.assertRaises(CuratorException, rio.do_action)
    def test_reindex_without_wait(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task
        rio = Reindex(self.ilo, testvars.reindex_basic,
            wait_for_completion=False)
        self.assertIsNone(rio.do_action())
    def test_reindex_timedout(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.incomplete_task
        rio = Reindex(self.ilo, testvars.reindex_basic,
             max_wait=1, wait_interval=1)
        self.assertRaises(FailedExecution, rio.do_action)
    def test_remote_with_no_host_key(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task
        # After building ilo, we need a different return value
        self.client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'irrelevant',
                'remote': {'wrong': 'invalid'}
            },
            'dest': { 'index': 'other_index' }
        }
        self.assertRaises(
            ConfigurationError, Reindex, self.ilo, badval)
    def test_remote_with_bad_host(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task
        # After building ilo, we need a different return value
        self.client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'irrelevant',
                'remote': {'host': 'invalid'}
            },
            'dest': { 'index': 'other_index' }
        }
        self.assertRaises(
            ConfigurationError, Reindex, self.ilo, badval)
    def test_remote_with_bad_url(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task
        # After building ilo, we need a different return value
        self.client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'irrelevant',
                'remote': {'host': 'asdf://hostname:1234'}
            },
            'dest': { 'index': 'other_index' }
        }
        self.assertRaises(
            ConfigurationError, Reindex, self.ilo, badval)
    def test_remote_with_bad_connection(self):
        self.builder()
        self.client.reindex.return_value = testvars.generic_task
        self.client.tasks.get.return_value = testvars.completed_task
        # After building ilo, we need a different return value
        self.client.indices.get_settings.return_value = {'other_index':{}}
        badval = {
            'source': {
                'index': 'REINDEX_SELECTION',
                'remote': {'host': 'https://example.org:XXXX'}
            },
            'dest': { 'index': 'other_index' }
        }
        urllib3 = Mock()
        urllib3.util.retry.side_effect = testvars.fake_fail
        self.assertRaises(Exception, Reindex, self.ilo, badval)
    def test_init_raise_empty_source_list(self):
        self.builder()
        badval = {
            'source': { 'index': [] },
            'dest': { 'index': 'other_index' }
        }
        rio = Reindex(self.ilo, badval)
        self.assertRaises(NoIndices, rio.do_action)
