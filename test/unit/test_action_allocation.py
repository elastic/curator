from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionAllocation(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, curator.Allocation, 'invalid')
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        ao = curator.Allocation(ilo, key='key', value='value')
        self.assertEqual(ilo, ao.index_list)
        self.assertEqual(client, ao.client)
    def test_create_body_no_key(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        self.assertRaises(curator.MissingArgument, curator.Allocation, ilo)
    def test_create_body_invalid_allocation_type(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        self.assertRaises(
            ValueError,
            curator.Allocation, ilo,
            key='key', value='value', allocation_type='invalid'
        )
    def test_create_body_valid(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        ao = curator.Allocation(ilo, key='key', value='value')
        self.assertEqual({'index.routing.allocation.require.key': 'value'}, ao.body)
    def test_do_action_raise_on_put_settings(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        client.indices.put_settings.side_effect = testvars.fake_fail
        ilo = curator.IndexList(client)
        ao = curator.Allocation(ilo, key='key', value='value')
        self.assertRaises(Exception, ao.do_action)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        ilo = curator.IndexList(client)
        ao = curator.Allocation(ilo, key='key', value='value')
        self.assertIsNone(ao.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        ilo = curator.IndexList(client)
        ao = curator.Allocation(ilo, key='key', value='value')
        self.assertIsNone(ao.do_action())
    def test_do_action_wait_v50(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        client.cluster.health.return_value = {'relocating_shards':0}
        ilo = curator.IndexList(client)
        ao = curator.Allocation(
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
        ilo = curator.IndexList(client)
        ao = curator.Allocation(
            ilo, key='key', value='value', wait_for_completion=True)
        self.assertIsNone(ao.do_action())
