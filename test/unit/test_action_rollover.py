from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionRollover(TestCase):
    def test_init_raise_bad_client(self):
        self.assertRaises(
            TypeError, curator.Rollover, 'invalid', 'name', {})
    def test_init_raise_bad_conditions(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        self.assertRaises(
            curator.ConfigurationError, curator.Rollover, client, 'name', 'string')
    def test_init_raise_bad_extra_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        self.assertRaises(
            curator.ConfigurationError, curator.Rollover, client, 'name',
            {'a':'b'}, None, 'string')
    def test_init_raise_non_rollable_index(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_alias.return_value = testvars.alias_retval
        self.assertRaises(
            ValueError, curator.Rollover, client, testvars.named_alias,
            {'a':'b'})
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_alias.return_value = testvars.rollable_alias
        client.indices.rollover.return_value = testvars.dry_run_rollover
        ro = curator.Rollover(
            client, testvars.named_alias, testvars.rollover_conditions)
        self.assertIsNone(ro.do_dry_run())
    def test_init_raise_max_size_on_unsupported_version(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '6.0.0'} }
        client.indices.get_alias.return_value = testvars.rollable_alias
        conditions = { 'max_size': '1g' }
        self.assertRaises(
            curator.ConfigurationError, curator.Rollover, client, 
            testvars.named_alias, conditions
        )
    def test_max_size_in_acceptable_verion(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '6.1.0'} }
        client.indices.get_alias.return_value = testvars.rollable_alias
        conditions = { 'max_size': '1g' }
        ro = curator.Rollover(client, testvars.named_alias, conditions)
        self.assertEqual(conditions, ro.conditions)
