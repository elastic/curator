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

class TestActionFileOpenClosed(CuratorTestCase):
    def test_open_closed(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.optionless_proto.format('open'))
        t1, t2 = ('dummy', 'my_index')
        self.create_index(t1)
        self.create_index(t2)
        # Decorators make this pylint exception necessary
        # pylint: disable=E1123
        self.client.indices.close(index=t2, ignore_unavailable=True)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        csi = self.client.cluster.state(metric='metadata')['metadata']['indices']
        self.assertNotEqual('close', csi[t2]['state'])
        self.assertNotEqual('close', csi[t1]['state'])
    def test_extra_option(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('open'))
        t1, t2 = ('dummy', 'my_index')
        self.create_index(t1)
        self.create_index(t2)
        # Decorators make this pylint exception necessary
        # pylint: disable=E1123
        self.client.indices.close(index=t2, ignore_unavailable=True)
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        csi = self.client.cluster.state(metric='metadata')['metadata']['indices']
        self.assertEqual('close', csi[t2]['state'])
        self.assertNotEqual('close', csi[t1]['state'])
        self.assertEqual(-1, result.exit_code)

class TestCLIOpenClosed(CuratorTestCase):
    def test_open_closed(self):
        t1, t2 = ('dummy', 'my_index')
        self.create_index(t1)
        self.create_index(t2)
        # Decorators make this pylint exception necessary
        # pylint: disable=E1123
        self.client.indices.close(index=t2, ignore_unavailable=True)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'open',
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIOpenClosed.test_open_closed'))
        csi = self.client.cluster.state(metric='metadata')['metadata']['indices']
        self.assertNotEqual('close', csi[t2]['state'])
        self.assertNotEqual('close', csi[t1]['state'])
        