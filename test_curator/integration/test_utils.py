import curator

from . import CuratorTestCase

class TestCloseIndex(CuratorTestCase):
    def test_positive(self):
        self.create_index('test_index')
        self.client.indices.close('test_index')

        self.assertTrue(curator.index_closed(self.client, 'test_index'))

    def test_negative(self):
        self.create_index('test_index')

        self.assertFalse(curator.index_closed(self.client, 'test_index'))

