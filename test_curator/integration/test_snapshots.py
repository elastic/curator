from datetime import datetime, timedelta
from curator import curator
import time

from mock import patch, Mock

from . import CuratorTestCase

class TestSnapshots(CuratorTestCase):
    def test_repository_will_be_created_and_deleted(self):
        self.assertTrue(curator._create_repository(self.client, self.args['repository'], curator.create_repo_body(self.args)))
        pre = curator._get_repository(self.client, self.args['repository'])
        self.assertEqual('fs', pre[self.args['repository']]['type'])
        curator._delete_repository(self.client, self.args['repository'])
        post = curator._get_repository(self.client, self.args['repository'])
        self.assertEqual(None, post)

    def test_snapshot_will_be_created_and_deleted(self):
        name = 'l-2014.05.22'
        self.create_repository(self.args['repository'])
        self.create_index(name)
        self.client.create(index=name, doc_type='log', body={'message':'TEST DOCUMENT'})
        curator._create_snapshot(self.client, name, prefix='l-', repo_name=self.args['repository'], argdict=self.args)
        result = curator._get_snapshot(self.client, self.args['repository'], name)
        self.assertEqual(name, result['snapshots'][0]['snapshot'])
        self.assertEqual('SUCCESS', result['snapshots'][0]['state'])
        curator._delete_snapshot(self.client, name, repo_name=self.args['repository'])
        snaps = self.client.snapshot.get(repository=self.args['repository'], snapshot='_all')
        self.assertEqual({"snapshots":[]}, snaps)

    def test_snapshot_will_not_be_created_twice(self):
        name = 'l-2014.05.22'
        self.create_repository(self.args['repository'])
        self.create_index(name)
        self.client.create(index=name, doc_type='log', body={'message':'TEST DOCUMENT'})
        curator._create_snapshot(self.client, name, prefix='l-', repo_name=self.args['repository'], argdict=self.args)
        result = curator._get_snapshot(self.client, self.args['repository'], name)
        self.assertEqual(name, result['snapshots'][0]['snapshot'])
        self.assertEqual('SUCCESS', result['snapshots'][0]['state'])
        self.client.create(index=name, doc_type='log', body={'message':'TEST DOCUMENT TWO'})
        # _create_snapshot will return True if it skipped the creation...
        self.assertTrue(curator._create_snapshot(self.client, name, prefix='l-', repo_name=self.args['repository'], argdict=self.args))

    def test_curator_will_create_and_delete_multiple_snapshots(self):
        self.create_indices(10)
        self.create_repository(self.args['repository'])
        expired = curator.find_expired_data(self.client, time_unit=self.args['time_unit'], unit_count=3, separator=self.args['separator'], prefix=self.args['prefix'])
        curator.index_loop(self.client, 'snapshot', expired, self.args['dry_run'], prefix=self.args['prefix'], argdict=self.args)
        result = curator._get_snapshot(self.client, self.args['repository'], '_all')
        self.assertEqual(7, len(result['snapshots']))
        expired = curator.find_expired_data(self.client, time_unit=self.args['time_unit'], unit_count=6, data_type='snapshot', separator=self.args['separator'], prefix=self.args['prefix'], repo_name=self.args['repository'])
        curator.index_loop(self.client, 'delete_snaps', expired, self.args['dry_run'], prefix=self.args['prefix'], repo_name=self.args['repository'])
        result = curator._get_snapshot(self.client, self.args['repository'], '_all')
        self.assertEqual(3, len(result['snapshots']))

    def test_curator_will_manage_hourly_snapshots(self):
        self.create_indices(10, 'hours')
        self.create_repository(self.args['repository'])
        expired = curator.find_expired_data(self.client, time_unit='hours', unit_count=3, separator=self.args['separator'], prefix=self.args['prefix'])
        curator.index_loop(self.client, 'snapshot', expired, self.args['dry_run'], prefix=self.args['prefix'], argdict=self.args)
        result = curator._get_snapshot(self.client, self.args['repository'], '_all')
        self.assertEqual(7, len(result['snapshots']))
        expired = curator.find_expired_data(self.client, time_unit='hours', unit_count=6, data_type='snapshot', separator=self.args['separator'], prefix=self.args['prefix'], repo_name=self.args['repository'])
        curator.index_loop(self.client, 'delete_snaps', expired, self.args['dry_run'], prefix=self.args['prefix'], repo_name=self.args['repository'])
        result = curator._get_snapshot(self.client, self.args['repository'], '_all')
        self.assertEqual(3, len(result['snapshots']))

    def test_curator_will_snap_latest_n_indices(self):
        self.create_indices(10)
        self.create_repository(self.args['repository'])
        curator.snap_latest_indices(self.client, 3, argdict=self.args)
        result = curator._get_snapshot(self.client, self.args['repository'], '_all')
        self.assertEqual(3, len(result['snapshots']))
        snapshots = [x['indices'][0] for x in result['snapshots']]
        indices = curator.get_indices(self.client)
        self.assertEqual(snapshots, indices[-3:])

