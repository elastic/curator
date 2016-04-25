from unittest import TestCase
from mock import Mock, patch
import elasticsearch
import curator
# Get test variables and constants from a single source
from . import testvars as testvars

class TestActionCreate_index(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, curator.CreateIndex, 'invalid')
    def test_init_raise_no_name(self):
        client = Mock()
        self.assertRaises(curator.ConfigurationError,
            curator.CreateIndex, client, None)
    def test_init(self):
        client = Mock()
        co = curator.CreateIndex(client, 'name')
        self.assertEqual('name', co.name)
        self.assertEqual(client, co.client)
    def test_do_dry_run(self):
        client = Mock()
        co = curator.CreateIndex(client, 'name')
        self.assertIsNone(co.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.indices.create.return_value = None
        co = curator.CreateIndex(client, 'name')
        self.assertIsNone(co.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.indices.create.return_value = None
        client.indices.create.side_effect = testvars.fake_fail
        co = curator.CreateIndex(client, 'name')
        self.assertRaises(curator.FailedExecution, co.do_action)
