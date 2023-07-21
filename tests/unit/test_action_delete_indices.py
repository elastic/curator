"""test_action_delete_indices"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from mock import Mock
from curator.actions import DeleteIndices
from curator.exceptions import FailedExecution
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionDeleteIndices(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def builder4(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_four
        self.client.indices.get_settings.return_value = testvars.settings_four
        self.client.indices.stats.return_value = testvars.stats_four
        self.client.indices.exists_alias.return_value = False
        self.client.indices.delete.return_value = None
        self.ilo = IndexList(self.client)
    def test_init_raise(self):
        self.assertRaises(TypeError, DeleteIndices, 'invalid')
    def test_init_raise_bad_master_timeout(self):
        self.builder()
        self.assertRaises(TypeError, DeleteIndices, self.ilo, 'invalid')
    def test_init(self):
        self.builder()
        dio = DeleteIndices(self.ilo)
        self.assertEqual(self.ilo, dio.index_list)
        self.assertEqual(self.client, dio.client)
    def test_do_dry_run(self):
        self.builder4()
        dio = DeleteIndices(self.ilo)
        self.assertIsNone(dio.do_dry_run())
    def test_do_action(self):
        self.builder4()
        dio = DeleteIndices(self.ilo)
        self.assertIsNone(dio.do_action())
    def test_do_action_not_successful(self):
        self.builder4()
        dio = DeleteIndices(self.ilo)
        self.assertIsNone(dio.do_action())
    def test_do_action_raises_exception(self):
        self.builder4()
        self.client.indices.delete.side_effect = testvars.fake_fail
        dio = DeleteIndices(self.ilo)
        self.assertRaises(FailedExecution, dio.do_action)
    def test_verify_result_positive(self):
        self.builder4()
        dio = DeleteIndices(self.ilo)
        self.assertTrue(dio._verify_result([],2))
