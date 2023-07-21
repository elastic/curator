"""Alias unit tests"""
# pylint: disable=missing-function-docstring, missing-class-docstring, invalid-name, line-too-long, attribute-defined-outside-init
from unittest import TestCase
from mock import Mock
from curator import IndexList
from curator.exceptions import ActionError, FailedExecution, MissingArgument, NoIndices
from curator.actions.alias import Alias
from . import testvars

class TestActionAlias(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def builder2(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_two
        self.client.indices.get_settings.return_value = testvars.settings_two
        self.client.indices.stats.return_value = testvars.stats_two
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_init_raise(self):
        self.assertRaises(MissingArgument, Alias)
    def test_add_raises_on_missing_parameter(self):
        self.builder()
        ao = Alias(name='alias')
        self.assertRaises(TypeError, ao.add)
    def test_add_raises_on_invalid_parameter(self):
        self.builder()
        ao = Alias(name='alias')
        self.assertRaises(TypeError, ao.add, [])
    def test_add_single(self):
        self.builder()
        ao = Alias(name='alias')
        ao.add(self.ilo)
        self.assertEqual(testvars.alias_one_add, ao.actions)
    def test_add_single_with_extra_settings(self):
        self.builder()
        esd = {
            'filter' : { 'term' : { 'user' : 'kimchy' } }
        }
        ao = Alias(name='alias', extra_settings=esd)
        ao.add(self.ilo)
        self.assertEqual(testvars.alias_one_add_with_extras, ao.actions)
    def test_remove_single(self):
        self.builder()
        self.client.indices.get_alias.return_value = testvars.settings_1_get_aliases
        ao = Alias(name='my_alias')
        ao.remove(self.ilo)
        self.assertEqual(testvars.alias_one_rm, ao.actions)
    def test_add_multiple(self):
        self.builder2()
        ao = Alias(name='alias')
        ao.add(self.ilo)
        cmp = sorted(ao.actions, key=lambda k: k['add']['index'])
        self.assertEqual(testvars.alias_two_add, cmp)
    def test_remove_multiple(self):
        self.builder2()
        self.client.indices.get_alias.return_value = testvars.settings_2_get_aliases
        ao = Alias(name='my_alias')
        ao.remove(self.ilo)
        cmp = sorted(ao.actions, key=lambda k: k['remove']['index'])
        self.assertEqual(testvars.alias_two_rm, cmp)
    def test_raise_action_error_on_empty_body(self):
        self.builder()
        ao = Alias(name='alias')
        self.assertRaises(ActionError, ao.check_actions)
    def test_raise_no_indices_on_empty_body_when_warn_if_no_indices(self):
        self.builder()
        # empty it, so there can be no body
        self.ilo.indices = []
        ao = Alias(name='alias')
        ao.add(self.ilo, warn_if_no_indices=True)
        self.assertRaises(NoIndices, ao.check_actions)
    def test_do_dry_run(self):
        self.builder()
        self.client.indices.update_aliases.return_value = testvars.alias_success
        ao = Alias(name='alias')
        ao.add(self.ilo)
        self.assertIsNone(ao.do_dry_run())
    def test_do_action(self):
        self.builder()
        self.client.indices.update_aliases.return_value = testvars.alias_success
        ao = Alias(name='alias')
        ao.add(self.ilo)
        self.assertIsNone(ao.do_action())
    def test_do_action_raises_exception(self):
        self.builder()
        self.client.indices.update_aliases.return_value = testvars.alias_success
        self.client.indices.update_aliases.side_effect = testvars.four_oh_one
        ao = Alias(name='alias')
        ao.add(self.ilo)
        self.assertRaises(FailedExecution, ao.do_action)
