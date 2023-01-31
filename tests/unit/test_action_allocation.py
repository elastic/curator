"""Alias unit tests"""
# pylint: disable=missing-function-docstring, missing-class-docstring, invalid-name, line-too-long
from unittest import TestCase
from mock import Mock
from curator import IndexList
from curator.exceptions import MissingArgument
from curator.actions.allocation import Allocation
from . import testvars

class TestActionAllocation(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, Allocation, 'invalid')
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        ao = Allocation(ilo, key='key', value='value')
        self.assertEqual(ilo, ao.index_list)
        self.assertEqual(client, ao.client)
    def test_create_body_no_key(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        self.assertRaises(MissingArgument, Allocation, ilo)
    def test_create_body_invalid_allocation_type(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        self.assertRaises(
            ValueError,
            Allocation, ilo,
            key='key', value='value', allocation_type='invalid'
        )
    def test_create_body_valid(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = IndexList(client)
        ao = Allocation(ilo, key='key', value='value')
        self.assertEqual({'index.routing.allocation.require.key': 'value'}, ao.settings)
    def test_do_action_raise_on_put_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        client.indices.put_settings.side_effect = testvars.fake_fail
        ilo = IndexList(client)
        ao = Allocation(ilo, key='key', value='value')
        self.assertRaises(Exception, ao.do_action)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        ilo = IndexList(client)
        ao = Allocation(ilo, key='key', value='value')
        self.assertIsNone(ao.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        ilo = IndexList(client)
        ao = Allocation(ilo, key='key', value='value')
        self.assertIsNone(ao.do_action())
    def test_do_action_wait_v50(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        client.cluster.health.return_value = {'relocating_shards':0}
        ilo = IndexList(client)
        ao = Allocation(
            ilo, key='key', value='value', wait_for_completion=True)
        self.assertIsNone(ao.do_action())
    def test_do_action_wait_v51(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.1.1'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        client.cluster.health.return_value = {'relocating_shards':0}
        ilo = IndexList(client)
        ao = Allocation(
            ilo, key='key', value='value', wait_for_completion=True)
        self.assertIsNone(ao.do_action())
