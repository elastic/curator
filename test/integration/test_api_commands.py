from curator import api as curator

from mock import patch, Mock

from . import CuratorTestCase

class TestAlias(CuratorTestCase):
    def test_add_to_pre_existent_alias(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_index('foo')
        curator.add_to_alias(self.client, 'foo', alias=alias)
        self.assertEquals(2, len(self.client.indices.get_alias(name=alias)))
    def test_add_to_non_existent_alias(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_index('foo')
        self.assertTrue(curator.add_to_alias(self.client, 'foo', alias="ooga"))
    def test_add_to_alias_with_closed(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_index('foo')
        curator.close_indices(self.client, 'foo')
        self.assertTrue(curator.index_closed(self.client, 'foo'))
        curator.add_to_alias(self.client, 'foo', alias=alias)
        self.assertFalse(curator.add_to_alias(self.client, 'foo', alias=alias))
    def test_add_to_alias_idx_already_in_alias(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_index('foo')
        curator.add_to_alias(self.client, 'foo', alias=alias)
        self.assertTrue(curator.add_to_alias(self.client, 'foo', alias=alias))
    def test_remove_from_alias_positive(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))
        self.create_index('foo')
        curator.add_to_alias(self.client, 'foo', alias=alias)
        self.assertEquals(2, len(self.client.indices.get_alias(name=alias)))
        curator.remove_from_alias(self.client, 'dummy', alias=alias)
        self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))
    def test_remove_from_alias_negative(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.assertFalse(curator.remove_from_alias(self.client, 'dummy', alias="ooga"))
    def test_full_alias_add_to_pre_existent_alias(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_index('foo')
        self.assertTrue(curator.alias(self.client, 'foo', alias=alias))
        self.assertEquals(2, len(self.client.indices.get_alias(name=alias)))
    def test_full_alias_add_non_existent_index(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.assertFalse(curator.alias(self.client, 'foo', alias='ooga'))
    def test_full_alias_remove_positive(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))
        self.create_index('foo')
        curator.add_to_alias(self.client, 'foo', alias=alias)
        self.assertEquals(2, len(self.client.indices.get_alias(name=alias)))
        curator.alias(self.client, 'dummy', alias=alias, remove=True)
        self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))
    def test_full_alias_remove_negative(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.assertFalse(curator.alias(self.client, 'dummy', alias="ooga", remove=True))

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

    def test_closed_index_replicas_cannot_be_modified(self):
        self.create_index('test_index')

        self.client.indices.close(index='test_index')
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

        self.assertEquals('0', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['number_of_replicas'])

        curator.change_replicas(self.client, 'test_index', replicas=1)

        self.assertEquals('0', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['number_of_replicas'])

        self.client.indices.open(index='test_index')
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('open', index_metadata['metadata']['indices']['test_index']['state'])

        self.assertEquals('0', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['number_of_replicas'])

class TestChangeAllocation(CuratorTestCase):
    def test_index_allocation_can_be_modified(self):
        self.create_index('test_index')

        curator.apply_allocation_rule(self.client, 'test_index', rule="key=value")

        self.assertEquals('value', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['routing']['allocation']['require']['key'])

    def test_index_allocation_can_be_modified_for_include(self):
        self.create_index('test_index')

        curator.apply_allocation_rule(self.client, 'test_index', rule="key=value", allocation_type='include')

        self.assertEquals('value', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['routing']['allocation']['include']['key'])

    def test_index_allocation_can_be_modified_for_exclude(self):
        self.create_index('test_index')

        curator.apply_allocation_rule(self.client, 'test_index', rule="key=value", allocation_type='exclude')

        self.assertEquals('value', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['routing']['allocation']['exclude']['key'])

    def test_closed_index_allocation_can_be_modified(self):
        self.create_index('test_index')

        self.client.indices.close(index='test_index')
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('close', index_metadata['metadata']['indices']['test_index']['state'])

        curator.apply_allocation_rule(self.client, 'test_index', rule="key=value")

        self.assertEquals('value', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['routing']['allocation']['require']['key'])

        self.client.indices.open(index='test_index')
        index_metadata = self.client.cluster.state(
            index='test_index',
            metric='metadata',
        )
        self.assertEquals('open', index_metadata['metadata']['indices']['test_index']['state'])

        self.assertEquals('value', self.client.indices.get_settings(index='test_index')['test_index']['settings']['index']['routing']['allocation']['require']['key'])


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
