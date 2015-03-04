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
