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
# '      repository: {0}\n'
# '      name: {1}\n'
# '      indices: {2}\n'
# '      include_aliases: {3}\n'
# '      ignore_unavailable: {4}\n'
# '      include_global_state: {5}\n'
# '      partial: {6}\n'
# '      rename_pattern: {7}\n'
# '      rename_replacement: {8}\n'
# '      extra_settings: {9}\n'
# '      wait_for_completion: {10}\n'
# '      skip_repo_fs_check: {11}\n'
# '      timeout_override: {12}\n'
class TestCLIRestore(CuratorTestCase):
    def test_restore(self):
        indices = []
        for i in range(1,4):
            self.add_docs('my_index{0}'.format(i))
            indices.append('my_index{0}'.format(i))
        snap_name = 'snapshot1'
        self.create_snapshot(snap_name, ','.join(indices))
        snapshot = curator.get_snapshot(
                    self.client, self.args['repository'], '_all'
                   )
        self.assertEqual(1, len(snapshot['snapshots']))
        self.client.indices.delete(','.join(indices))
        self.assertEqual([], curator.get_indices(self.client))
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.restore_snapshot_proto.format(
                self.args['repository'],
                snap_name,
                indices,
                False,
                False,
                True,
                False,
                ' ',
                ' ',
                ' ',
                True,
                False,
                301
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        restored_indices = sorted(curator.get_indices(self.client))
        self.assertEqual(indices, restored_indices)
    def test_restore_with_rename(self):
        indices = []
        for i in range(1,4):
            self.add_docs('my_index{0}'.format(i))
            indices.append('my_index{0}'.format(i))
        snap_name = 'snapshot1'
        self.create_snapshot(snap_name, ','.join(indices))
        snapshot = curator.get_snapshot(
                    self.client, self.args['repository'], '_all'
                   )
        self.assertEqual(1, len(snapshot['snapshots']))
        self.client.indices.delete(','.join(indices))
        self.assertEqual([], curator.get_indices(self.client))
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.restore_snapshot_proto.format(
                self.args['repository'],
                snap_name,
                indices,
                False,
                False,
                True,
                False,
                'my_index(.+)',
                'new_index$1',
                ' ',
                True,
                False,
                301
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        restored_indices = sorted(curator.get_indices(self.client))
        self.assertEqual(
            ['new_index1', 'new_index2', 'new_index3'],
            restored_indices
        )
