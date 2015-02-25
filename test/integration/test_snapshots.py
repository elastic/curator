from datetime import datetime, timedelta
from curator import curator
import time

from mock import patch, Mock

from . import CuratorTestCase

class TestSnapshots(CuratorTestCase):

    def test_snapshot_will_be_created_and_deleted(self):
        name = 'l-2014.05.22'
        self.create_repository()
        self.create_index(name)
        self.client.create(index=name, doc_type='log', body={'message':'TEST DOCUMENT'})
        curator.create_snapshot(self.client, snapshot_name=name, indices=name, repository=self.args['repository'])
        result = curator.get_snapshot(self.client, self.args['repository'], name)
        self.assertEqual(name, result['snapshots'][0]['snapshot'])
        self.assertEqual('SUCCESS', result['snapshots'][0]['state'])
        curator.delete_snapshot(self.client, name, repository=self.args['repository'])
        snaps = self.client.snapshot.get(repository=self.args['repository'], snapshot='_all')
        self.assertEqual({"snapshots":[]}, snaps)

    def test_snapshot_will_not_be_created_twice(self):
        name = 'l-2014.05.22'
        self.create_repository()
        self.create_index(name)
        self.client.create(index=name, doc_type='log', body={'message':'TEST DOCUMENT'})
        curator.create_snapshot(self.client, snapshot_name=name, indices=name, repository=self.args['repository'])
        result = curator.get_snapshot(self.client, self.args['repository'], name)
        self.assertEqual(name, result['snapshots'][0]['snapshot'])
        self.assertEqual('SUCCESS', result['snapshots'][0]['state'])
        self.client.create(index=name, doc_type='log', body={'message':'TEST DOCUMENT TWO'})
        # create_snapshot will return True if it skipped the creation...
        self.assertTrue(curator.create_snapshot(self.client, snapshot_name=name, indices=name, repository=self.args['repository']))

    def test_curator_will_create_multiple_snapshots(self):
        self.create_indices(10)
        self.create_repository()
        for i in range(3,0,-1):
            curator.snapshot(self.client, older_than=i, timestring='%Y.%m.%d', repository=self.args['repository'])
            time.sleep(1)
        # Test two ways of getting results to ensure both return properly
        result = curator.get_snapshot(self.client, self.args['repository'], '_all')
        result2 = curator.get_object_list(self.client, data_type='snapshot', repository=self.args['repository'])
        self.assertEqual(3, len(result['snapshots']))
        self.assertEqual(3, len(result2))

    def test_curator_will_snap_latest_n_indices(self):
        self.create_indices(10)
        self.create_repository()
        curator.snapshot(self.client, most_recent=3, repository=self.args['repository'])
        result = curator.get_snapshot(self.client, self.args['repository'], '_all')
        snapped_indices = result['snapshots'][0]['indices']
        indices = curator.get_indices(self.client)
        self.assertEqual(snapped_indices, indices[-3:])

    def test_curator_will_snap_all_indices(self):
        self.create_indices(10)
        self.create_repository()
        curator.snapshot(self.client, all_indices=True, repository=self.args['repository'])
        result = curator.get_snapshot(self.client, self.args['repository'], '_all')
        snapped_indices = result['snapshots'][0]['indices']
        snapped_indices.sort()
        indices = curator.get_indices(self.client)
        indices.sort()
        self.assertEqual(snapped_indices, indices)

    def test_curator_will_ignore_wrong_timestamp(self):
        self.create_indices(10)
        self.create_indices(10, 'hours')
        self.create_repository()
        curator.snapshot(self.client, older_than=1, timestring='%Y.%m.%d', repository=self.args['repository'])
        result = curator.get_snapshot(self.client, self.args['repository'], '_all')
        snapped_indices = result['snapshots'][0]['indices']
        self.assertEqual(9, len(snapped_indices))

    def test_curator_will_match_snapshot_prefix(self):
        self.create_indices(10)
        self.create_repository()
        curator.snapshot(self.client, most_recent=3, snapshot_prefix='foo-', repository=self.args['repository'])
        curator.snapshot(self.client, most_recent=6, snapshot_prefix='bar-', repository=self.args['repository'])
        result = curator.get_snapshot(self.client, self.args['repository'], '_all')
        for snap in result['snapshots']:
            if snap['snapshot'].startswith("foo-"):
                foo_indices = snap['indices']
            elif snap['snapshot'].startswith("bar-"):
                bar_indices = snap['indices']
        self.assertEqual(3, len(foo_indices))
        self.assertEqual(6, len(bar_indices))