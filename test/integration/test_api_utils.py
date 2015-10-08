from curator import api as curator

from mock import patch, Mock

from . import CuratorTestCase

class TestGetIndices(CuratorTestCase):
    def test_positive(self):
        self.create_index('test_index1')
        self.create_index('test_index2')
        l = sorted(curator.get_indices(self.client))
        r = ["test_index1", "test_index2"]
        self.assertEqual(r, l)
    def test_negative(self):
        l = sorted(curator.get_indices(self.client))
        r = ["test_index1", "test_index2"]
        self.assertNotEqual(r, l)
    def test_exception(self):
        client = "foo"
        l = curator.get_indices(client)
        self.assertFalse(l)

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

class TestPruneClosed(CuratorTestCase):
    def test_positive(self):
        for i in range(1, 11):
            self.create_index('test_index{0}'.format(i))
        self.client.indices.close(index='test_index1,test_index2,test_index3')
        l = curator.get_indices(self.client)
        l = sorted(curator.prune_closed(self.client, l))
        self.assertEqual(7, len(l))
        r = [
            "test_index10", "test_index4", "test_index5", "test_index6",
            "test_index7", "test_index8", "test_index9",
            ]
        self.assertEqual(l, r)
    def test_negative(self):
        for i in range(1, 11):
            self.create_index('test_index{0}'.format(i))
        self.client.indices.delete(index='test_index1,test_index2,test_index3')
        l = curator.get_indices(self.client)
        l = sorted(curator.prune_closed(self.client, l))
        self.assertEqual(7, len(l))
        r = [
            "test_index10", "test_index4", "test_index5", "test_index6",
            "test_index7", "test_index8", "test_index9",
            ]
        self.assertEqual(l, r)

class TestPruneOpened(CuratorTestCase):
    def test_positive(self):
        for i in range(1, 11):
            self.create_index('test_index{0}'.format(i))
        self.client.indices.close(index='test_index1,test_index2,test_index3')
        l = curator.get_indices(self.client)
        l = sorted(curator.prune_opened(self.client, l))
        self.assertEqual(3, len(l))
        r = [
            "test_index1", "test_index2", "test_index3"
            ]
        self.assertEqual(l, r)
    def test_negative(self):
        for i in range(1, 11):
            self.create_index('test_index{0}'.format(i))
        self.client.indices.delete(index='test_index1,test_index2,test_index3')
        self.client.indices.close(index='test_index4')
        l = curator.get_indices(self.client)
        l = sorted(curator.prune_opened(self.client, l))
        self.assertEqual(1, len(l))
        r = ["test_index4"]
        self.assertEqual(l, r)


class TestSegmentCount(CuratorTestCase):
    def test_simple(self):
        self.create_index('test_index', shards=2)
        self.client.index(index='test_index', doc_type='t', id=42, body={})
        self.client.indices.refresh(index='test_index')
        self.assertEquals((2, 1), curator.get_segmentcount(self.client, 'test_index'))
