import elasticsearch
import curator
import os
import click
from click import testing as clicktest
from mock import patch, Mock

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

class TestCLIUtilsFilterCallback(CuratorTestCase):
    def test_filter_callback_without_timestring(self):
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        'show',
                        'indices',
                        '--older-than', '5',
                        '--time-unit', 'days',
                    ],
                    obj={"filters":[]})
        self.assertEqual(1, result.exit_code)
    def test_filter_callback_without_timeunit(self):
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        'show',
                        'indices',
                        '--newer-than', '5',
                        '--timestring', '%Y.%m.%d',
                    ],
                    obj={"filters":[]})
        self.assertEqual(1, result.exit_code)

class TestSSLFlags(CuratorTestCase):
    def test_bad_certificate(self):
        self.create_indices(10)
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        '--use_ssl',
                        '--certificate', '/path/to/nowhere',
                        'show',
                        'indices',
                        '--older-than', '5',
                        '--time-unit', 'days',
                        '--timestring', '%Y.%m.%d',
                    ],
                    obj={"filters":[]})
        self.assertEqual(1, result.exit_code)
        self.assertEqual('Error: Could not open certificate at /path/to/nowhere\n', result.output)
    def test_ssl_no_validate(self):
        self.create_indices(10)
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--logfile', os.devnull,
                        '--host', host,
                        '--port', str(port),
                        '--use_ssl',
                        '--ssl-no-validate',
                        'show',
                        'indices',
                        '--older-than', '5',
                        '--time-unit', 'days',
                        '--timestring', '%Y.%m.%d',
                    ],
                    obj={"filters":[]})
        self.assertTrue(1, result.exit_code)
