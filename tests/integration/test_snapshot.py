"""Test snapshot action functionality"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
from datetime import datetime, timedelta
from curator.helpers.getters import get_indices, get_snapshot
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')

class TestActionFileSnapshot(CuratorTestCase):
    def test_snapshot(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = 'snapshot1'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.snapshot_test.format(self.args['repository'], snap_name, 1, 30))
        self.invoke_runner()
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 1 == len(snapshot['snapshots'])
        assert snap_name == snapshot['snapshots'][0]['snapshot']
    def test_snapshot_datemath(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = '<snapshot-{now-1d/d}>'
        snap_name_parsed = f"snapshot-{(datetime.utcnow()-timedelta(days=1)).strftime('%Y.%m.%d')}"
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.snapshot_test.format(self.args['repository'], snap_name, 1, 30))
        self.invoke_runner()
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 1 == len(snapshot['snapshots'])
        assert snap_name_parsed == snapshot['snapshots'][0]['snapshot']
    def test_snapshot_ignore_empty_list(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = 'snapshot1'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.test_682.format(self.args['repository'], snap_name, True, 1, 30))
        self.invoke_runner()
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 0 == len(snapshot['snapshots'])
        assert 0 == len(get_indices(self.client))
    def test_snapshot_do_not_ignore_empty_list(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = 'snapshot1'
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.test_682.format(self.args['repository'], snap_name, False, 1, 30))
        self.invoke_runner()
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 0 == len(snapshot['snapshots'])
        assert 5 == len(get_indices(self.client))
    def test_no_repository(self):
        self.create_indices(5)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.snapshot_test.format(' ', 'snap_name', 1, 30))
        self.invoke_runner()
        assert 1 == self.result.exit_code
    def test_extra_option(self):
        self.create_indices(5)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.bad_option_proto_test.format('snapshot'))
        self.invoke_runner()
        assert 1 == self.result.exit_code

class TestCLISnapshot(CuratorTestCase):
    def test_snapshot(self):
        self.create_indices(5)
        self.create_repository()
        snap_name = 'snapshot1'
        args = self.get_runner_args()
        args += [
            '--config', self.args['configfile'],
            'snapshot',
            '--repository', self.args['repository'],
            '--name', snap_name,
            '--wait_interval', '1',
            '--max_wait', '30',
            '--filter_list', '{"filtertype":"none"}',
        ]
        assert 0 == self.run_subprocess(args, logname='TestCLISnapshot.test_snapshot')
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 1 == len(snapshot['snapshots'])
        assert snap_name == snapshot['snapshots'][0]['snapshot']
