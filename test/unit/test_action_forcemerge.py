from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionForceMerge(TestCase):
    def test_init_raise_bad_client(self):
        self.assertRaises(
            TypeError, curator.ForceMerge, 'invalid', max_num_segments=2)
    def test_init_raise_no_segment_count(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        ilo = curator.IndexList(client)
        self.assertRaises(
            curator.MissingArgument, curator.ForceMerge, ilo)
    def test_init(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        ilo = curator.IndexList(client)
        fmo = curator.ForceMerge(ilo, max_num_segments=2)
        self.assertEqual(ilo, fmo.index_list)
        self.assertEqual(client, fmo.client)
    def test_do_dry_run(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        client.indices.forcemerge.return_value = None
        client.indices.optimize.return_value = None
        ilo = curator.IndexList(client)
        fmo = curator.ForceMerge(ilo, max_num_segments=2)
        self.assertIsNone(fmo.do_dry_run())
    def test_do_action_pre5(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        client.info.return_value = {'version': {'number': '2.3.2'} }
        client.indices.optimize.return_value = None
        ilo = curator.IndexList(client)
        fmo = curator.ForceMerge(ilo, max_num_segments=2)
        self.assertIsNone(fmo.do_action())
    def test_do_action(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.forcemerge.return_value = None
        ilo = curator.IndexList(client)
        fmo = curator.ForceMerge(ilo, max_num_segments=2)
        self.assertIsNone(fmo.do_action())
    def test_do_action_with_delay(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.forcemerge.return_value = None
        ilo = curator.IndexList(client)
        fmo = curator.ForceMerge(ilo, max_num_segments=2, delay=0.050)
        self.assertIsNone(fmo.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '5.0.0'} }
        client.indices.get_settings.return_value = testvars.settings_one
        client.cluster.state.return_value = testvars.clu_state_one
        client.indices.stats.return_value = testvars.stats_one
        client.indices.segments.return_value = testvars.shards
        client.indices.forcemerge.return_value = None
        client.indices.optimize.return_value = None
        client.indices.forcemerge.side_effect = testvars.fake_fail
        client.indices.optimize.side_effect = testvars.fake_fail
        ilo = curator.IndexList(client)
        fmo = curator.ForceMerge(ilo, max_num_segments=2)
        self.assertRaises(curator.FailedExecution, fmo.do_action)
