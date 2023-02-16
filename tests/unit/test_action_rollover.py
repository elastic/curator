"""test_action_rollover"""
from unittest import TestCase
from mock import Mock
from curator.actions import Rollover
from curator.exceptions import ConfigurationError

# Get test variables and constants from a single source
from . import testvars

class TestActionRollover(TestCase):
    def test_init_raise_bad_client(self):
        self.assertRaises(TypeError, Rollover, 'invalid', 'name', {})
    def test_init_raise_bad_conditions(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        self.assertRaises(ConfigurationError, Rollover, client, 'name', 'string')
    def test_init_raise_bad_extra_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        self.assertRaises(ConfigurationError, Rollover, client, 'name', {'a':'b'}, None, 'string')
    def test_init_raise_non_rollable_index(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_alias.return_value = testvars.alias_retval
        self.assertRaises(
            ValueError, Rollover, client, testvars.named_alias,
            {'a':'b'})
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_alias.return_value = testvars.rollable_alias
        client.indices.rollover.return_value = testvars.dry_run_rollover
        ro = Rollover(
            client, testvars.named_alias, testvars.rollover_conditions)
        self.assertIsNone(ro.do_dry_run())
    def test_max_size_in_acceptable_verion(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '6.1.0'} }
        client.indices.get_alias.return_value = testvars.rollable_alias
        conditions = { 'max_size': '1g' }
        ro = Rollover(client, testvars.named_alias, conditions)
        self.assertEqual(conditions, ro.conditions)
