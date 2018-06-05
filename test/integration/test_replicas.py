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

class TestActionFileReplicas(CuratorTestCase):
    def test_increase_count(self):
        count = 2
        idx = 'my_index'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.replicas_test.format(count))
        self.create_index(idx)
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(
            count,
            int(self.client.indices.get_settings(
                index=idx)[idx]['settings']['index']['number_of_replicas'])
        )
    def test_no_count(self):
        self.create_index('foo')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.replicas_test.format(' '))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, _.exit_code)
    def test_extra_option(self):
        self.create_index('foo')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('replicas'))
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, _.exit_code)

class TestCLIReplicas(CuratorTestCase):
    def test_increase_count(self):
        count = 2
        idx = 'my_index'
        self.create_index(idx)
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'replicas',
            '--count', str(count),
            '--filter_list', '{"filtertype":"pattern","kind":"prefix","value":"my"}',
        ]
        self.assertEqual(0, self.run_subprocess(args, logname='TestCLIOpenClosed.test_open_closed'))
        self.assertEqual(
            count,
            int(self.client.indices.get_settings(index=idx)[idx]['settings']['index']['number_of_replicas'])
        )