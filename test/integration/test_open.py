import elasticsearch
import curator
import os
import json
import string, random, tempfile
import click
from click import testing as clicktest
from mock import patch, Mock

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestCLIOpenClosed(CuratorTestCase):
    def test_open_closed(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('open'))
        self.create_index('my_index')
        self.client.indices.close(
            index='my_index', ignore_unavailable=True)
        self.create_index('dummy')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='my_index',
                metric='metadata',
            )['metadata']['indices']['my_index']['state']
        )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='dummy',
                metric='metadata',
            )['metadata']['indices']['dummy']['state']
        )
    def test_open_opened(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('open'))
        self.create_index('my_index')
        self.create_index('dummy')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='my_index',
                metric='metadata',
            )['metadata']['indices']['my_index']['state']
        )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='dummy',
                metric='metadata',
            )['metadata']['indices']['dummy']['state']
        )
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('open'))
        self.create_index('my_index')
        self.client.indices.close(
            index='my_index', ignore_unavailable=True)
        self.create_index('dummy')
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(
            'close',
            self.client.cluster.state(
                index='my_index',
                metric='metadata',
            )['metadata']['indices']['my_index']['state']
        )
        self.assertNotEqual(
            'close',
            self.client.cluster.state(
                index='dummy',
                metric='metadata',
            )['metadata']['indices']['dummy']['state']
        )
        self.assertEqual(-1, result.exit_code)
