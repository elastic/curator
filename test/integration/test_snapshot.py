import elasticsearch
import curator
import os
import json
import string, random, tempfile
from click import testing as clicktest
from mock import patch, Mock

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

class TestCLISnapshot(CuratorTestCase):
    def test_snapshot(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = 'snapshot1'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.snapshot_test.format(self.args['repository'], snap_name, 1, 30))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        snapshot = curator.get_snapshot(
                    self.client, self.args['repository'], '_all'
                   )
        self.assertEqual(1, len(snapshot['snapshots']))
        self.assertEqual(snap_name, snapshot['snapshots'][0]['snapshot'])
    def test_snapshot_ignore_empty_list(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = 'snapshot1'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.test_682.format(self.args['repository'], snap_name, True, 1, 30))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        snapshot = curator.get_snapshot(
                    self.client, self.args['repository'], '_all'
                   )
        self.assertEqual(0, len(snapshot['snapshots']))
        self.assertEquals(0, len(curator.get_indices(self.client)))
    def test_snapshot_do_not_ignore_empty_list(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = 'snapshot1'
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.test_682.format(self.args['repository'], snap_name, False, 1, 30))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        snapshot = curator.get_snapshot(
                    self.client, self.args['repository'], '_all'
                   )
        self.assertEqual(0, len(snapshot['snapshots']))
        self.assertEquals(5, len(curator.get_indices(self.client)))
    def test_no_repository(self):
        self.create_indices(5)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.snapshot_test.format(' ', 'snap_name', 1, 30))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_extra_option(self):
        self.create_indices(5)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('snapshot'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
