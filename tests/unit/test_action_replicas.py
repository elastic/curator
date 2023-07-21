"""test_action_replicas"""
# pylint: disable=missing-function-docstring, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest import TestCase
from mock import Mock
from curator.actions import Replicas
from curator.exceptions import FailedExecution, MissingArgument
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

class TestActionReplicas(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = self.VERSION
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.client.indices.put_settings.return_value = None
        self.ilo = IndexList(self.client)
    def test_init_raise_bad_client(self):
        self.assertRaises(TypeError, Replicas, 'invalid', count=2)
    def test_init_raise_no_count(self):
        self.builder()
        self.assertRaises(MissingArgument, Replicas, self.ilo)
    def test_init(self):
        self.builder()
        rpo = Replicas(self.ilo, count=2)
        self.assertEqual(self.ilo, rpo.index_list)
        self.assertEqual(self.client, rpo.client)
    def test_do_dry_run(self):
        self.builder()
        rpo = Replicas(self.ilo, count=0)
        self.assertIsNone(rpo.do_dry_run())
    def test_do_action(self):
        self.builder()
        rpo = Replicas(self.ilo, count=0)
        self.assertIsNone(rpo.do_action())
    def test_do_action_wait(self):
        self.builder()
        self.client.cluster.health.return_value = {'status':'green'}
        rpo = Replicas(self.ilo, count=1, wait_for_completion=True)
        self.assertIsNone(rpo.do_action())
    def test_do_action_raises_exception(self):
        self.builder()
        self.client.indices.segments.return_value = testvars.shards
        self.client.indices.put_settings.side_effect = testvars.fake_fail
        rpo = Replicas(self.ilo, count=2)
        self.assertRaises(FailedExecution, rpo.do_action)
