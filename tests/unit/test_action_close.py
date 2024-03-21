"""Alias unit tests"""
# pylint: disable=missing-function-docstring, missing-class-docstring, invalid-name, line-too-long, attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock
from curator import IndexList
from curator.exceptions import FailedExecution
from curator.actions.close import Close
from . import testvars

class TestActionClose(TestCase):
    VERSION = {'version': {'number': '5.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.flush_synced.return_value = testvars.synced_pass
        self.client.indices.exists_alias.return_value = False
        self.client.indices.close.return_value = None
        self.ilo = IndexList(self.client)
    def test_init_raise(self):
        self.assertRaises(TypeError, Close, 'invalid')
    def test_init(self):
        self.builder()
        self.client.indices.flush_synced.return_value = None
        self.client.indices.close.return_value = None
        clo = Close(self.ilo)
        self.assertEqual(self.ilo, clo.index_list)
        self.assertEqual(self.client, clo.client)
    def test_do_dry_run(self):
        self.builder()
        self.ilo = IndexList(self.client)
        clo = Close(self.ilo)
        self.assertIsNone(clo.do_dry_run())
    def test_do_action(self):
        self.builder()
        self.ilo = IndexList(self.client)
        clo = Close(self.ilo)
        self.assertIsNone(clo.do_action())
    def test_do_action_with_delete_aliases(self):
        self.builder()
        self.ilo = IndexList(self.client)
        clo = Close(self.ilo, delete_aliases=True)
        self.assertIsNone(clo.do_action())
    def test_do_action_with_skip_flush(self):
        self.builder()
        self.ilo = IndexList(self.client)
        clo = Close(self.ilo, skip_flush=True)
        self.assertIsNone(clo.do_action())
    def test_do_action_raises_exception(self):
        self.builder()
        self.client.indices.close.side_effect = testvars.fake_fail
        self.ilo = IndexList(self.client)
        clo = Close(self.ilo)
        self.assertRaises(FailedExecution, clo.do_action)
    def test_do_action_delete_aliases_with_exception(self):
        self.builder()
        self.ilo = IndexList(self.client)
        self.client.indices.delete_alias.side_effect = testvars.fake_fail
        clo = Close(self.ilo, delete_aliases=True)
        self.assertIsNone(clo.do_action())
