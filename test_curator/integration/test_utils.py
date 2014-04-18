from curator import curator

from mock import patch, Mock

from . import CuratorTestCase

class TestCloseIndex(CuratorTestCase):
    def test_positive(self):
        self.create_index('test_index')
        self.client.indices.close('test_index')

        self.assertTrue(curator.index_closed(self.client, 'test_index'))

    def test_negative(self):
        self.create_index('test_index')

        self.assertFalse(curator.index_closed(self.client, 'test_index'))

class TestCloseIndex(CuratorTestCase):
    def test_index_will_be_closed(self):
        self.create_index('test_index')
        self.assertIsNone(curator._close_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

    def test_closed_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.indices.close(index='test_index')
        self.assertTrue(curator._close_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

class TestDeleteIndex(CuratorTestCase):
    def test_index_will_be_deleted(self):
        self.create_index('test_index')
        self.assertIsNone(curator._delete_index(self.client, 'test_index'))
        self.assertFalse(self.client.indices.exists('test_index'))

class TestBloomIndex(CuratorTestCase):
    def test_bloom_filter_will_be_disabled(self):
        self.create_index('test_index')
        self.assertIsNone(curator._bloom_index(self.client, 'test_index'))

        settings = self.client.indices.get_settings(index='test_index')
        self.assertEquals('false', settings['test_index']['settings']['index']['codec']['bloom']['load'])

    def test_closed_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.indices.close(index='test_index')
        self.assertTrue(curator._bloom_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

class TestOptimizeIndex(CuratorTestCase):
    def test_closed_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.indices.close(index='test_index')
        self.assertTrue(curator._optimize_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

    @patch('curator.curator.get_segmentcount')
    def test_optimized_index_will_be_skipped(self, get_segmentcount):
        get_segmentcount.return_value = 1, 4
        self.create_index('test_index')
        self.assertTrue(curator._optimize_index(self.client, 'test_index', max_num_segments=4))
        get_segmentcount.assert_called_once_with(self.client, 'test_index')

    @patch('curator.curator.index_closed')
    @patch('curator.curator.get_segmentcount')
    def test_unoptimized_index_will_be_optimized(self, get_segmentcount, index_closed):
        get_segmentcount.return_value = 1, 40
        index_closed.return_value = False
        client = Mock()
        self.create_index('test_index')
        self.assertIsNone(curator._optimize_index(client, 'test_index', max_num_segments=4))
        get_segmentcount.assert_called_once_with(client, 'test_index')
        index_closed.assert_called_once_with(client, 'test_index')
        client.indices.optimize.assert_called_once_with(index='test_index', max_num_segments=4)

class TestSegmentCount(CuratorTestCase):
    def test_simple(self):
        self.create_index('test_index', shards=2)
        self.client.index(index='test_index', doc_type='t', id=42, body={})
        self.client.indices.refresh(index='test_index')

        self.assertEquals((2, 1), curator.get_segmentcount(self.client, 'test_index'))

class TestExcludeIndex(CuratorTestCase):
    def test_index_will_be_excluded_by_simple_pattern(self):
        self.create_index('logstash-2014.06.07')
        self.create_index('logstash-2014.06.08')
        self.create_index('logstash-2014.06.09')
        object_list = curator.get_object_list(self.client, data_type='index', prefix='logstash-', repository=None, exclude_pattern='2014.06.08')
        self.assertEquals(
            [
                u'logstash-2014.06.07',
                u'logstash-2014.06.09',
            ],
            object_list
        )
