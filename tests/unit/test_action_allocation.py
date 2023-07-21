"""Alias unit tests"""
# pylint: disable=missing-function-docstring, missing-class-docstring, invalid-name, line-too-long, attribute-defined-outside-init
from unittest import TestCase
from mock import Mock
from curator import IndexList
from curator.exceptions import MissingArgument
from curator.actions.allocation import Allocation
from . import testvars

class TestActionAllocation(TestCase):
    VERSION = {'version': {'number': '5.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.client.indices.put_settings.return_value = None
        self.ilo = IndexList(self.client)
    def test_init_raise(self):
        self.assertRaises(TypeError, Allocation, 'invalid')
    def test_init(self):
        self.builder()
        ao = Allocation(self.ilo, key='key', value='value')
        self.assertEqual(self.ilo, ao.index_list)
        self.assertEqual(self.client, ao.client)
    def test_create_body_no_key(self):
        self.builder()
        self.assertRaises(MissingArgument, Allocation, self.ilo)
    def test_create_body_invalid_allocation_type(self):
        self.builder()
        self.assertRaises(
            ValueError,
            Allocation, self.ilo,
            key='key', value='value', allocation_type='invalid'
        )
    def test_create_body_valid(self):
        self.builder()
        ao = Allocation(self.ilo, key='key', value='value')
        self.assertEqual({'index.routing.allocation.require.key': 'value'}, ao.settings)
    def test_do_action_raise_on_put_settings(self):
        self.builder()
        self.client.indices.put_settings.side_effect = testvars.fake_fail
        ao = Allocation(self.ilo, key='key', value='value')
        self.assertRaises(Exception, ao.do_action)
    def test_do_dry_run(self):
        self.builder()
        alo = Allocation(self.ilo, key='key', value='value')
        self.assertIsNone(alo.do_dry_run())
    def test_do_action(self):
        self.builder()
        alo = Allocation(self.ilo, key='key', value='value')
        self.assertIsNone(alo.do_action())
    def test_do_action_wait_v50(self):
        self.builder()
        self.client.cluster.health.return_value = {'relocating_shards':0}
        alo = Allocation(
            self.ilo, key='key', value='value', wait_for_completion=True)
        self.assertIsNone(alo.do_action())
    def test_do_action_wait_v51(self):
        self.builder()
        self.client.info.return_value = {'version': {'number': '5.1.1'} }
        self.client.cluster.health.return_value = {'relocating_shards':0}
        alo = Allocation(
            self.ilo, key='key', value='value', wait_for_completion=True)
        self.assertIsNone(alo.do_action())
