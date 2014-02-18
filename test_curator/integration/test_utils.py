from curator import curator

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
        curator._close_index(self.client, 'test_index')
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
        curator._delete_index(self.client, 'test_index')
        self.assertFalse(self.client.indices.exists('test_index'))

class TestBloomIndex(CuratorTestCase):
    def test_bloom_filter_will_be_disabled(self):
        self.create_index('test_index')
        curator._bloom_index(self.client, 'test_index')

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
