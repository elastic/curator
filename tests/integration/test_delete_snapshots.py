"""Test delete snapshot functionality"""

# pylint: disable=C0115, C0116, invalid-name
import os
import time
from curator import IndexList
from curator.actions.snapshot import Snapshot
from curator.helpers.getters import get_snapshot
from . import CuratorTestCase
from . import testvars

DEBUG_LEVEL = '3'

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
# '      repository: {0}\n'
# '      - filtertype: {1}\n'
# '        source: {2}\n'
# '        direction: {3}\n'
# '        timestring: {4}\n'
# '        unit: {5}\n'
# '        unit_count: {6}\n'
# '        epoch: {7}\n')


class TestActionFileDeleteSnapshots(CuratorTestCase):
    def test_deletesnapshot(self):
        # Create snapshots to delete and verify them
        self.create_repository()
        timestamps = []
        for i in range(1, 4):
            self.add_docs(f'my_index{i}')
            ilo = IndexList(self.client)
            snap = Snapshot(
                ilo,
                repository=self.args['repository'],
                name='curator-%Y%m%d%H%M%S',
                wait_interval=1,
            )
            snap.do_action()
            snapshot = get_snapshot(self.client, self.args['repository'], '*')
            assert i == len(snapshot['snapshots'])
            time.sleep(1.0)
            timestamps.append(int(time.time()))
            time.sleep(1.0)
        # Setup the actual delete
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_snap_proto.format(
                self.args['repository'],
                'age',
                'creation_date',
                'older',
                ' ',
                'seconds',
                '0',
                timestamps[0],
            ),
        )
        self.invoke_runner()
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 2 == len(snapshot['snapshots'])

    def test_no_repository(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_snap_proto.format(
                ' ', 'age', 'creation_date', 'older', ' ', 'seconds', '0', ' '
            ),
        )
        self.invoke_runner()
        assert 1 == self.result.exit_code  # type: ignore[union-attr]

    def test_extra_options(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.bad_option_proto_test.format('delete_snapshots'),
        )
        self.invoke_runner()
        assert 1 == self.result.exit_code  # type: ignore[union-attr]


class TestCLIDeleteSnapshots(CuratorTestCase):
    def test_deletesnapshot(self):
        # Create snapshots to delete and verify them
        self.create_repository()
        timestamps = []
        for i in range(1, 4):
            self.add_docs(f'my_index{i}')
            ilo = IndexList(self.client)
            snap = Snapshot(
                ilo,
                repository=self.args['repository'],
                name='curator-%Y%m%d%H%M%S',
                wait_interval=1,
            )
            snap.do_action()
            snapshot = get_snapshot(self.client, self.args['repository'], '*')
            assert i == len(snapshot['snapshots'])
            time.sleep(1.0)
            timestamps.append(int(time.time()))
            time.sleep(1.0)
        filter_list = (
            '{"filtertype":"age","source":"creation_date","direction":"older",'
            '"unit":"seconds","unit_count":0,"epoch":' + str(timestamps[0]) + '}'
        )
        # Setup the actual delete
        args = self.get_runner_args()
        args += [
            '--blacklist',
            'urllib3',
            '--blacklist',
            'elastic_transport',
            '--debug-level',
            DEBUG_LEVEL,
            '--config',
            self.args['configfile'],
            'delete-snapshots',
            '--repository',
            self.args['repository'],
            '--filter_list',
            filter_list,
        ]
        assert 0 == self.run_subprocess(
            args, logname='TestCLIDeleteSnapshots.test_deletesnapshot'
        )
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 2 == len(snapshot['snapshots'])

    def test_count_by_age(self):
        self.create_repository()
        timestamps = []

        def add_snap(num, name):
            self.add_docs(f'my_index{num}')
            ilo = IndexList(self.client)
            snap = Snapshot(
                ilo, repository=self.args['repository'], name=name, wait_interval=0.5
            )
            snap.do_action()
            snapshot = get_snapshot(self.client, self.args['repository'], '*')
            assert num == len(snapshot['snapshots'])
            time.sleep(1.0)
            timestamps.append(int(time.time()))
            time.sleep(1.0)

        name = 'curator-%Y%m%d%H%M%S'
        for i in range(1, 4):
            add_snap(i, name)
        add_snap(4, 'kibana-index')
        # Setup the actual delete
        filter_list = (
            '{"filtertype":"count","count":2,"use_age":true,"source":"name",'
            '"timestring":"%Y%m%d%H%M%S"}'
        )
        args = self.get_runner_args()
        args += [
            '--blacklist',
            'urllib3.connectionpool',
            '--blacklist',
            'elastic_transport.transport',
            '--debug-level',
            DEBUG_LEVEL,
            '--config',
            self.args['configfile'],
            'delete-snapshots',
            '--repository',
            self.args['repository'],
            '--filter_list',
            filter_list,
        ]
        assert 0 == self.run_subprocess(
            args, logname='TestCLIDeleteSnapshots.test_deletesnapshot'
        )
        snapshot = get_snapshot(self.client, self.args['repository'], '*')
        assert 3 == len(snapshot['snapshots'])
