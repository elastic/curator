"""test_action_rollover"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from unittest.mock import Mock
from curator.actions import Rollover
from curator.exceptions import ConfigurationError

# Get test variables and constants from a single source
from . import testvars

class TestActionRollover(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
    def test_init_raise_bad_client(self):
        self.assertRaises(TypeError, Rollover, 'invalid', 'name', {})
    def test_init_raise_bad_conditions(self):
        self.builder()
        self.assertRaises(ConfigurationError, Rollover, self.client, 'name', 'string')
    def test_init_raise_bad_extra_settings(self):
        self.builder()
        self.assertRaises(
            ConfigurationError, Rollover, self.client, 'name', {'a':'b'}, None, 'string')
    def test_init_raise_non_rollable_index(self):
        self.builder()
        self.client.indices.get_alias.return_value = testvars.alias_retval
        self.assertRaises(ValueError, Rollover, self.client, testvars.named_alias, {'a':'b'})
    def test_do_dry_run(self):
        self.builder()
        self.client.indices.get_alias.return_value = testvars.rollable_alias
        self.client.indices.rollover.return_value = testvars.dry_run_rollover
        rlo = Rollover(self.client, testvars.named_alias, testvars.rollover_conditions)
        self.assertIsNone(rlo.do_dry_run())
    def test_max_size_in_acceptable_verion(self):
        self.builder()
        self.client.indices.get_alias.return_value = testvars.rollable_alias
        conditions = { 'max_size': '1g' }
        rlo = Rollover(self.client, testvars.named_alias, conditions)
        self.assertEqual(conditions, rlo.conditions)
