from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionReplicas(TestCase):
    def test_init_raise_bad_client(self):
        self.assertRaises(
            TypeError, curator.Replicas, 'invalid', count=2)
    def test_init_raise_no_count(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        ilo = curator.IndexList(client)
        self.assertRaises(
            curator.MissingArgument, curator.Replicas, ilo)
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        ilo = curator.IndexList(client)
        ro = curator.Replicas(ilo, count=2)
        self.assertEqual(ilo, ro.index_list)
        self.assertEqual(client, ro.client)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        ilo = curator.IndexList(client)
        ro = curator.Replicas(ilo, count=0)
        self.assertIsNone(ro.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        ilo = curator.IndexList(client)
        ro = curator.Replicas(ilo, count=0)
        self.assertIsNone(ro.do_action())
    def test_do_action_wait(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.put_settings.return_value = None
        client.cluster.health.return_value = {'status':'green'}
        ilo = curator.IndexList(client)
        ro = curator.Replicas(ilo, count=1, wait_for_completion=True)
        self.assertIsNone(ro.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        client.indices.put_settings.return_value = None
        client.indices.put_settings.side_effect = testvars.fake_fail
        ilo = curator.IndexList(client)
        ro = curator.Replicas(ilo, count=2)
        self.assertRaises(curator.FailedExecution, ro.do_action)
