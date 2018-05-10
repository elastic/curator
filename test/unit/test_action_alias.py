from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionAlias(TestCase):
    def test_init_raise(self):
        self.assertRaises(curator.MissingArgument, curator.Alias)
    def test_add_raises_on_missing_parameter(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        self.assertRaises(TypeError, ao.add)
    def test_add_raises_on_invalid_parameter(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        self.assertRaises(TypeError, ao.add, [])
    def test_add_single(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        ao.add(_)
        self.assertEqual(testvars.alias_one_add, ao.actions)
    def test_add_single_with_extra_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        _ = curator.IndexList(client)
        esd = {
            'filter' : { 'term' : { 'user' : 'kimchy' } }
        }
        ao = curator.Alias(name='alias', extra_settings=esd)
        ao.add(_)
        self.assertEqual(testvars.alias_one_add_with_extras, ao.actions)
    def test_remove_single(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.get_alias.return_value = testvars.settings_1_get_aliases
        _ = curator.IndexList(client)
        ao = curator.Alias(name='my_alias')
        ao.remove(_)
        self.assertEqual(testvars.alias_one_rm, ao.actions)
    def test_add_multiple(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_two
        client.cluster.state.return_value = testvars.clu_state_two
        client.indices.stats.return_value = testvars.stats_two
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        ao.add(_)
        cmp = sorted(ao.actions, key=lambda k: k['add']['index'])
        self.assertEqual(testvars.alias_two_add, cmp)
    def test_remove_multiple(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_two
        client.cluster.state.return_value = testvars.clu_state_two
        client.indices.stats.return_value = testvars.stats_two
        client.indices.get_alias.return_value = testvars.settings_2_get_aliases
        _ = curator.IndexList(client)
        ao = curator.Alias(name='my_alias')
        ao.remove(_)
        cmp = sorted(ao.actions, key=lambda k: k['remove']['index'])
        self.assertEqual(testvars.alias_two_rm, cmp)
    def test_raise_action_error_on_empty_body(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        self.assertRaises(curator.ActionError, ao.body)
    def test_raise_no_indices_on_empty_body_when_warn_if_no_indices(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        _ = curator.IndexList(client)
        # empty it, so there can be no body
        _.indices = []
        ao = curator.Alias(name='alias')
        ao.add(_, warn_if_no_indices=True)
        self.assertRaises(curator.NoIndices, ao.body)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.update_aliases.return_value = testvars.alias_success
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        ao.add(_)
        self.assertIsNone(ao.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.update_aliases.return_value = testvars.alias_success
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        ao.add(_)
        self.assertIsNone(ao.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.update_aliases.return_value = testvars.alias_success
        client.indices.update_aliases.side_effect = testvars.four_oh_one
        _ = curator.IndexList(client)
        ao = curator.Alias(name='alias')
        ao.add(_)
        self.assertRaises(curator.FailedExecution, ao.do_action)
