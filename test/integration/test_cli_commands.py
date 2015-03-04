import elasticsearch
import curator
import os
import click
from click import testing as clicktest
from mock import patch, Mock
from scripttest import TestFileEnvironment

from . import CuratorTestCase

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestGetClient(CuratorTestCase):
    def test_get_client_positive(self):
        client_args = {"host":host, "port":port}
        client = curator.get_client(**client_args)
        self.assertTrue(isinstance(client, elasticsearch.client.Elasticsearch))
    def test_get_client_negative_connection_fail(self):
        client_args = {"host":host, "port":54321}
        with self.assertRaises(SystemExit) as cm:
            curator.get_client(**client_args)
        self.assertEqual(cm.exception.code, 1)

class TestCLIShow(CuratorTestCase):
    def test_cli_show_indices(self):
        self.create_indices(10)
        indices = curator.get_indices(self.client)
        expected = sorted(indices, reverse=True)[:4]
        env = TestFileEnvironment('./scratch')
        script = os.path.join(os.path.dirname(__file__), '../../run_curator.py')
        result = env.run("python " + script + " --logfile /dev/null --host " + host + " --port " + str(port) + " show indices --newer-than 5 --timestring '%Y.%m.%d' --time-unit days")
        output = sorted(result.stdout.splitlines(), reverse=True)
        self.assertEqual(expected, output)

class TestCLIAlias(CuratorTestCase):
    def test_cli_alias_fail(self):
        # Testing for failure because --name is omitted.  An exit code of 1 is validated.
        env = TestFileEnvironment('./scratch')
        script = os.path.join(os.path.dirname(__file__), '../../run_curator.py')
        result = env.run("python " + script + " --logfile /dev/null --host " + host + " --port " + str(port) + " alias indices --newer-than 5 --timestring '%Y.%m.%d' --time-unit days", expect_error=True)
        self.assertEqual(1, result.returncode)

class TestCLIAllocate(CuratorTestCase):
    def test_cli_allocate_fail(self):
        # Testing for failure because --rule is omitted.  An exit code of 1 is validated.
        env = TestFileEnvironment('./scratch')
        script = os.path.join(os.path.dirname(__file__), '../../run_curator.py')
        result = env.run("python " + script + " --logfile /dev/null --host " + host + " --port " + str(port) + " allocation indices --newer-than 5 --timestring '%Y.%m.%d' --time-unit days", expect_error=True)
        self.assertEqual(1, result.returncode)

class TestCLIReplicas(CuratorTestCase):
    def test_cli_replicas_fail(self):
        # Testing for failure because --count is omitted.  An exit code of 1 is validated.
        env = TestFileEnvironment('./scratch')
        script = os.path.join(os.path.dirname(__file__), '../../run_curator.py')
        result = env.run("python " + script + " --logfile /dev/null --host " + host + " --port " + str(port) + " replicas indices --newer-than 5 --timestring '%Y.%m.%d' --time-unit days", expect_error=True)
        self.assertEqual(1, result.returncode)

class TestCLISnapshot(CuratorTestCase):
    def test_cli_snapshot_fail(self):
        # Testing for failure because --repository is omitted.  An exit code of 1 is validated.
        env = TestFileEnvironment('./scratch')
        script = os.path.join(os.path.dirname(__file__), '../../run_curator.py')
        result = env.run("python " + script + " --logfile /dev/null --host " + host + " --port " + str(port) + " snapshot indices --newer-than 5 --timestring '%Y.%m.%d' --time-unit days", expect_error=True)
        self.assertEqual(1, result.returncode)
