from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionAllocation(TestCase):
    def test_bad_client(self):
        self.assertRaises(TypeError, curator.ClusterRouting, 'invalid', setting='enable')
    def test_bad_setting(self):
        client = Mock()
        self.assertRaises(
            ValueError, curator.ClusterRouting, client, setting='invalid'
        )
    def test_bad_routing_type(self):
        client = Mock()
        self.assertRaises(
            ValueError,
            curator.ClusterRouting,
            client,
            routing_type='invalid',
            setting='enable'
        )
    def test_bad_value_with_allocation(self):
        client = Mock()
        self.assertRaises(
            ValueError,
            curator.ClusterRouting,
            client,
            routing_type='allocation',
            setting='enable',
            value='invalid'
        )
    def test_bad_value_with_rebalance(self):
        client = Mock()
        self.assertRaises(
            ValueError,
            curator.ClusterRouting,
            client,
            routing_type='rebalance',
            setting='enable',
            value='invalid'
        )
    def test_do_dry_run(self):
        client = Mock()
        cro = curator.ClusterRouting(
            client,
            routing_type='allocation',
            setting='enable',
            value='all'
        )
        self.assertIsNone(cro.do_dry_run())
    def test_do_action_raise_on_put_settings(self):
        client = Mock()
        client.cluster.put_settings.return_value = None
        client.cluster.put_settings.side_effect = testvars.fake_fail
        cro = curator.ClusterRouting(
            client,
            routing_type='allocation',
            setting='enable',
            value='all'
        )
        self.assertRaises(Exception, cro.do_action)
    def test_do_action_wait(self):
        client = Mock()
        client.cluster.put_settings.return_value = None
        client.cluster.health.return_value = {'relocating_shards':0}
        cro = curator.ClusterRouting(
            client,
            routing_type='allocation',
            setting='enable',
            value='all',
            wait_for_completion=True
        )
        self.assertIsNone(cro.do_action())
