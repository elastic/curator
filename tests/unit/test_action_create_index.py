"""Unit tests for create_index action"""
from unittest import TestCase
from mock import Mock
from curator.actions import CreateIndex
from curator.exceptions import ConfigurationError, FailedExecution
# Get test variables and constants from a single source
from . import testvars

class TestActionCreate_index(TestCase):
    def test_init_raise(self):
        self.assertRaises(TypeError, CreateIndex, name='name')
    def test_init_raise_no_name(self):
        client = Mock()
        self.assertRaises(ConfigurationError,
            CreateIndex, client, name=None)
    def test_init(self):
        client = Mock()
        co = CreateIndex(client, name='name')
        self.assertEqual('name', co.name)
        self.assertEqual(client, co.client)
    def test_do_dry_run(self):
        client = Mock()
        co = CreateIndex(client, name='name')
        self.assertIsNone(co.do_dry_run())
    def test_do_action(self):
        client = Mock()
        client.indices.create.return_value = None
        co = CreateIndex(client, name='name')
        self.assertIsNone(co.do_action())
    def test_do_action_raises_exception(self):
        client = Mock()
        client.indices.create.return_value = None
        client.indices.create.side_effect = testvars.fake_fail
        co = CreateIndex(client, name='name')
        self.assertRaises(FailedExecution, co.do_action)
