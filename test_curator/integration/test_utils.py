from curator import curator

from mock import patch, Mock

from . import CuratorTestCase

class TestChangeReplicas(CuratorTestCase):
    def test_index_replicas_can_be_modified(self):
        self.create_index('test_index')
        self.assertEquals('0', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['number_of_replicas'])

        curator.change_replicas(self.client, 'test_index', replicas=1)

        self.assertEquals('1', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['number_of_replicas'])

    def test_index_replicas_untouched(self):
        self.create_index('test_index')
        self.assertEquals('0', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['number_of_replicas'])

        curator.change_replicas(self.client, 'test_index', replicas=0)

        self.assertEquals('0', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['number_of_replicas'])

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
        self.assertIsNone(curator.close_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

    def test_closed_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.indices.close(index='test_index')
        self.assertTrue(curator.close_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

class TestOpenIndex(CuratorTestCase):
    def test_index_will_not_be_opened(self):
        #don't open an already opened index.
        self.create_index('test_index')
        self.assertIsNone(curator.open_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata'
        )
        self.assertEquals('open', index_metadata['metadata']['indices']['test_index']['state'])

    def test_close_open_index(self):
        self.create_index('test_index')
        self.assertIsNone(curator.close_index(self.client, 'test_index'))
        #let's make sure it's closed.
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

        #it's closed, now open it.
        self.assertIsNone(curator.open_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata'
        )
        self.assertEquals('open', index_metadata['metadata']['indices']['test_index']['state'])

class TestShardsPerNode(CuratorTestCase):
    def test_set_shards_per_node(self):
        self.create_index('logstash-2014.06.07')
        self.assertIsNone(curator.shards_per_node(self.client, shards_per_node=-1, older_than=1,
                                                  time_unit='days', prefix='logstash', timestring='%%Y.%%m.%%d'))

class TestDeleteIndex(CuratorTestCase):
    def test_index_will_be_deleted(self):
        self.create_index('test_index')
        self.assertIsNone(curator.delete_index(self.client, 'test_index'))
        self.assertFalse(self.client.indices.exists('test_index'))

class TestBloomIndex(CuratorTestCase):
    def test_bloom_filter_will_be_disabled(self):
        self.create_index('test_index')
        self.assertIsNone(curator.disable_bloom_filter(self.client, 'test_index'))
        # Bloom filters have been removed from the 1.x branch after 1.4.0
        no_more_bloom = (1, 4, 0)
        version_number = curator.get_version(self.client)
        if version_number < no_more_bloom:
            settings = self.client.indices.get_settings(index='test_index')
            self.assertEquals('false', settings['test_index']['settings']['index']['codec']['bloom']['load'])

    def test_closed_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.indices.close(index='test_index')
        self.assertTrue(curator.disable_bloom_filter(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

class TestOptimizeIndex(CuratorTestCase):
    def test_closed_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.indices.close(index='test_index')
        self.assertTrue(curator.optimize_index(self.client, 'test_index'))
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

    @patch('curator.curator.get_segmentcount')
    def test_optimized_index_will_be_skipped(self, get_segmentcount):
        get_segmentcount.return_value = 1, 4
        self.create_index('test_index')
        self.assertTrue(curator.optimize_index(self.client, 'test_index', max_num_segments=4))
        get_segmentcount.assert_called_once_with(self.client, 'test_index')

    @patch('curator.curator.index_closed')
    @patch('curator.curator.get_segmentcount')
    def test_unoptimized_index_will_be_optimized(self, get_segmentcount, index_closed):
        get_segmentcount.return_value = 1, 40
        index_closed.return_value = False
        client = Mock()
        self.create_index('test_index')
        self.assertIsNone(curator.optimize_index(client, 'test_index', max_num_segments=4))
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
        object_list = curator.get_object_list(self.client, data_type='index', prefix='logstash-', suffix='', repository=None, exclude_pattern='2014.06.08')
        self.assertEquals(
            [
                u'logstash-2014.06.07',
                u'logstash-2014.06.09',
            ],
            object_list
        )
