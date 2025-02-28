"""Test snapshot restore functionality"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
from curator.helpers.getters import get_indices, get_snapshot
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

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
# '      wait_interval: {13}\n'
# '      max_wait: {14}\n'


class TestActionFileRestore(CuratorTestCase):
    """Test file-based configuration restore operations"""

    def test_restore(self):
        """Test restore action"""
        indices = []
        for i in range(1, 4):
            self.add_docs(f'my_index{i}')
            indices.append(f'my_index{i}')
        snap_name = 'snapshot1'
        self.create_snapshot(snap_name, ','.join(indices))
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        self.assertEqual(1, len(snapshot['snapshots']))
        assert 1 == len(snapshot['snapshots'])
        self.client.indices.delete(index=','.join(indices))
        self.assertEqual([], get_indices(self.client))
        assert not get_indices(self.client)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
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
                301,
                1,
                3,
            ),
        )
        self.invoke_runner()
        restored_indices = sorted(get_indices(self.client))
        self.assertEqual(indices, restored_indices)
        assert indices == restored_indices
        # The test runs so fast that it tries to execute the cleanup step
        # and delete the repository before Elasticsearch is actually ready
        time.sleep(0.5)

    def test_restore_with_rename(self):
        """Test restore action with renaming enabled"""
        indices = []
        for i in range(1, 4):
            self.add_docs(f'my_index{i}')
            indices.append(f'my_index{i}')
        snap_name = 'snapshot1'
        self.create_snapshot(snap_name, ','.join(indices))
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        time.sleep(1)
        self.assertEqual(1, len(snapshot['snapshots']))
        assert 1 == len(snapshot['snapshots'])
        self.client.indices.delete(index=','.join(indices))
        self.assertEqual([], get_indices(self.client))
        assert not get_indices(self.client)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
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
                301,
                1,
                3,
            ),
        )
        self.invoke_runner()
        time.sleep(1)
        restored_indices = sorted(get_indices(self.client))
        self.assertEqual(['new_index1', 'new_index2', 'new_index3'], restored_indices)
        assert ['new_index1', 'new_index2', 'new_index3'] == restored_indices
        # The test runs so fast that it tries to execute the cleanup step
        # and delete the repository before Elasticsearch is actually ready
        time.sleep(1)

    def test_restore_wildcard(self):
        """Test restore with wildcard"""
        indices = []
        my_indices = []
        wildcard = ['my_*']
        for i in range(1, 4):
            for prefix in ['my_', 'not_my_']:
                self.add_docs(f'{prefix}index{i}')
                indices.append(f'{prefix}index{i}')
                if prefix == 'my_':
                    my_indices.append(f'{prefix}index{i}')
        snap_name = 'snapshot1'
        self.create_snapshot(snap_name, ','.join(indices))
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        self.assertEqual(1, len(snapshot['snapshots']))
        assert 1 == len(snapshot['snapshots'])
        self.client.indices.delete(index=','.join(indices))
        self.assertEqual([], get_indices(self.client))
        assert not get_indices(self.client)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.restore_snapshot_proto.format(
                self.args['repository'],
                snap_name,
                wildcard,
                False,
                False,
                True,
                False,
                ' ',
                ' ',
                ' ',
                True,
                False,
                301,
                1,
                3,
            ),
        )
        self.invoke_runner()
        restored_indices = sorted(get_indices(self.client))
        self.assertEqual(my_indices, restored_indices)
        assert my_indices == restored_indices
        # The test runs so fast that it tries to execute the cleanup step
        # and delete the repository before Elasticsearch is actually ready
        time.sleep(0.5)


class TestCLIRestore(CuratorTestCase):
    def test_restore(self):
        indices = []
        for i in range(1, 4):
            self.add_docs(f'my_index{i}')
            indices.append(f'my_index{i}')
        snap_name = 'snapshot1'
        self.create_snapshot(snap_name, ','.join(indices))
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        self.assertEqual(1, len(snapshot['snapshots']))
        assert 1 == len(snapshot['snapshots'])
        self.client.indices.delete(index=f'{",".join(indices)}')
        self.assertEqual([], get_indices(self.client))
        assert not get_indices(self.client)
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'restore',
            '--repository',
            self.args['repository'],
            '--name',
            snap_name,
            '--index',
            indices[0],
            '--index',
            indices[1],
            '--index',
            indices[2],
            '--wait_interval',
            '1',
            '--max_wait',
            '3',
            '--filter_list',
            '{"filtertype":"none"}',
        ]
        # self.assertEqual(
        #     0, self.run_subprocess(args, logname='TestCLIRestore.test_restore')
        # )
        assert 0 == self.run_subprocess(args, logname='TestCLIRestore.test_restore')
        restored_indices = sorted(get_indices(self.client))
        self.assertEqual(indices, restored_indices)
        assert indices == restored_indices
        # The test runs so fast that it tries to execute the cleanup step
        # and delete the repository before Elasticsearch is actually ready
        time.sleep(0.5)
