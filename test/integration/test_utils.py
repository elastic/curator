from curator import api as curator

from mock import patch, Mock

from . import CuratorTestCase

class TestCloseIndex1(CuratorTestCase):
    def test_positive(self):
        self.create_index('test_index')
        self.client.indices.close('test_index')

        self.assertTrue(curator.index_closed(self.client, 'test_index'))

    def test_negative(self):
        self.create_index('test_index')

        self.assertFalse(curator.index_closed(self.client, 'test_index'))

class TestCloseIndex2(CuratorTestCase):
    def test_index_will_be_closed(self):
        self.create_index('test_index')
        self.assertTrue(curator.close_indices(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

    def test_closed_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.indices.close(index='test_index')
        self.assertTrue(curator.close_indices(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

class TestSegmentCount(CuratorTestCase):
    def test_simple(self):
        self.create_index('test_index', shards=2)
        self.client.index(index='test_index', doc_type='t', id=42, body={})
        self.client.indices.refresh(index='test_index')

        self.assertEquals((2, 1), curator.get_segmentcount(self.client, 'test_index'))
