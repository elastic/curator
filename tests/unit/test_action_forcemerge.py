"""test_action_forcemerge"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from mock import Mock
from curator.actions import ForceMerge
from curator.exceptions import FailedExecution, MissingArgument
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionForceMerge(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.client.indices.segments.return_value = testvars.shards
        self.ilo = IndexList(self.client)
    def test_init_raise_bad_client(self):
        self.assertRaises(
            TypeError, ForceMerge, 'invalid', max_num_segments=2)
    def test_init_raise_no_segment_count(self):
        self.builder()
        self.assertRaises(MissingArgument, ForceMerge, self.ilo)
    def test_init(self):
        self.builder()
        fmo = ForceMerge(self.ilo, max_num_segments=2)
        self.assertEqual(self.ilo, fmo.index_list)
        self.assertEqual(self.client, fmo.client)
    def test_do_dry_run(self):
        self.builder()
        self.client.indices.forcemerge.return_value = None
        self.client.indices.optimize.return_value = None
        fmo = ForceMerge(self.ilo, max_num_segments=2)
        self.assertIsNone(fmo.do_dry_run())
    def test_do_action(self):
        self.builder()
        self.client.indices.forcemerge.return_value = None
        fmo = ForceMerge(self.ilo, max_num_segments=2)
        self.assertIsNone(fmo.do_action())
    def test_do_action_with_delay(self):
        self.builder()
        self.client.indices.forcemerge.return_value = None
        fmo = ForceMerge(self.ilo, max_num_segments=2, delay=0.050)
        self.assertIsNone(fmo.do_action())
    def test_do_action_raises_exception(self):
        self.builder()
        self.client.indices.forcemerge.return_value = None
        self.client.indices.optimize.return_value = None
        self.client.indices.forcemerge.side_effect = testvars.fake_fail
        self.client.indices.optimize.side_effect = testvars.fake_fail
        fmo = ForceMerge(self.ilo, max_num_segments=2)
        self.assertRaises(FailedExecution, fmo.do_action)
