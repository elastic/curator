"""test_action_open"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock
from curator.actions import Open
from curator.exceptions import FailedExecution
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionOpen(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_four
        self.client.indices.get_settings.return_value = testvars.settings_four
        self.client.indices.stats.return_value = testvars.stats_four
        self.client.indices.exists_alias.return_value = False
        self.client.indices.open.return_value = None
        self.ilo = IndexList(self.client)
    def test_init_raise(self):
        self.assertRaises(TypeError, Open, 'invalid')
    def test_init(self):
        self.builder()
        opn = Open(self.ilo)
        self.assertEqual(self.ilo, opn.index_list)
        self.assertEqual(self.client, opn.client)
    def test_do_dry_run(self):
        self.builder()
        self.ilo.filter_opened()
        opn = Open(self.ilo)
        self.assertEqual(['c-2016.03.05'], opn.index_list.indices)
        self.assertIsNone(opn.do_dry_run())
    def test_do_action(self):
        self.builder()
        self.ilo.filter_opened()
        opn = Open(self.ilo)
        self.assertEqual(['c-2016.03.05'], opn.index_list.indices)
        self.assertIsNone(opn.do_action())
    def test_do_action_raises_exception(self):
        self.builder()
        self.client.indices.open.side_effect = testvars.fake_fail
        opn = Open(self.ilo)
        self.assertRaises(FailedExecution, opn.do_action)
