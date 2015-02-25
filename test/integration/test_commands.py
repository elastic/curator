from curator import api as curator

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

class TestDeleteIndex(CuratorTestCase):
    def test_index_will_be_deleted(self):
        self.create_index('test_index')
        self.assertTrue(curator.delete_indices(self.client, 'test_index'))
        self.assertFalse(self.client.indices.exists('test_index'))

class TestBloomIndex(CuratorTestCase):
    def test_bloom_filter_will_be_disabled(self):
        self.create_index('test_index')
        # Bloom filters have been removed from the 1.x branch after 1.4.0
        no_more_bloom = (1, 4, 0)
        version_number = curator.get_version(self.client)
        if version_number < no_more_bloom:
            self.assertTrue(curator.disable_bloom_filter(self.client, 'test_index'))
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
    def test_optimized_index_will_be_skipped(self):
        self.create_index('test_index')
        self.client.create(index='test_index', doc_type='log', body={'message':'TEST DOCUMENT'})
        # Will only have 1 segment
        self.assertTrue(curator.optimize_index(self.client, 'test_index', max_num_segments=4))

    def test_unoptimized_index_will_be_optimized(self):
        self.create_index('test_index')
        for i in range(1, 11):
            self.client.create(index='test_index', doc_type='log' + str(i), body={'message':'TEST DOCUMENT'})
            curator.optimize(self.client, "test_index", max_num_segments=10, delay=1)
        # Make sure we have more than 4 segments before the optimize
        self.assertGreater(curator.get_segmentcount(self.client, "test_index")[1], 4 )
        self.assertTrue(curator.optimize_index(self.client, 'test_index', max_num_segments=1))
        self.assertEqual(1, curator.get_segmentcount(self.client, "test_index")[1] )
