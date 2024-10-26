"""test_action_shrink"""

# pylint: disable=C0103,C0115,C0116,W0201,W0212
from unittest import TestCase
from unittest.mock import Mock
from curator.actions import Shrink
from curator.exceptions import ActionError, ConfigurationError
from curator import IndexList

# Get test variables and constants from a single source
from . import testvars


class TestActionShrink_ilo(TestCase):
    def test_init_raise_bad_client(self):
        self.assertRaises(TypeError, Shrink, 'invalid')


class TestActionShrink_extra_settings(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)

    def test_extra_settings_1(self):
        self.builder()
        self.assertRaises(
            ConfigurationError,
            Shrink,
            self.ilo,
            extra_settings={'settings': {'foobar'}},
        )

    def test_extra_settings_2(self):
        self.builder()
        self.assertRaises(
            ConfigurationError, Shrink, self.ilo, extra_settings={'foobar'}
        )


class TestActionShrink_data_node(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.node_name = 'node_name'
        self.node_id = 'my_node'
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'name': self.node_name}}
        }
        self.ilo = IndexList(self.client)
        self.shrink = Shrink(self.ilo)

    def test_non_data_node(self):
        self.builder()
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'roles': ['ingest'], 'name': self.node_name}}
        }
        self.assertFalse(self.shrink._data_node(self.node_id))

    def test_data_node(self):
        self.builder()
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'roles': ['data'], 'name': self.node_name}}
        }
        self.assertTrue(self.shrink._data_node(self.node_id))


class TestActionShrink_exclude_node(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.node_name = 'node_name'
        self.ilo = IndexList(self.client)

    def test_positive(self):
        self.builder()
        node_filters = {'exclude_nodes': [self.node_name]}
        shrink = Shrink(self.ilo, node_filters=node_filters)
        self.assertTrue(shrink._exclude_node(self.node_name))

    def test_negative(self):
        self.builder()
        node_filters = {'exclude_nodes': ['not_this_node']}
        shrink = Shrink(self.ilo, node_filters=node_filters)
        self.assertFalse(shrink._exclude_node(self.node_name))


class TestActionShrink_qualify_single_node(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.node_name = 'node_name'
        self.node_id = 'my_node'
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'name': self.node_name}}
        }
        self.ilo = IndexList(self.client)

    def test_positive(self):
        self.builder()
        byte_count = 123456
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'roles': ['data'], 'name': self.node_name}}
        }
        self.client.nodes.stats.return_value = {
            'nodes': {
                self.node_id: {
                    'fs': {
                        'data': ['one'],
                        'total': {'available_in_bytes': byte_count},
                    },
                    'name': self.node_name,
                }
            }
        }
        shrink = Shrink(self.ilo, shrink_node=self.node_name)
        shrink.qualify_single_node()
        self.assertEqual(byte_count, shrink.shrink_node_avail)

    def test_not_found(self):
        self.builder()
        shrink = Shrink(self.ilo, shrink_node='not_me')
        self.assertRaises(ConfigurationError, shrink.qualify_single_node)

    def test_excluded(self):
        self.builder()
        node_filters = {'exclude_nodes': [self.node_name]}
        shrink = Shrink(self.ilo, shrink_node=self.node_name, node_filters=node_filters)
        self.assertRaises(ConfigurationError, shrink.qualify_single_node)

    def test_non_data_node(self):
        self.builder()
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'roles': ['ingest'], 'name': self.node_name}}
        }
        shrink = Shrink(self.ilo, shrink_node=self.node_name)
        self.assertRaises(ActionError, shrink.qualify_single_node)


class TestActionShrink_most_available_node(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.node_name = 'node_name'
        self.node_id = 'my_node'
        self.byte_count = 123456
        self.client.nodes.stats.return_value = {
            'nodes': {self.node_id: {'name': self.node_name}}
        }
        self.ilo = IndexList(self.client)

    def test_excluded(self):
        self.builder()
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'roles': ['data']}}
        }
        self.client.nodes.stats.return_value = {
            'nodes': {
                self.node_id: {
                    'fs': {
                        'data': ['one'],
                        'total': {'available_in_bytes': self.byte_count},
                    },
                    'name': self.node_name,
                }
            }
        }
        node_filters = {'exclude_nodes': [self.node_name]}
        shrink = Shrink(self.ilo, node_filters=node_filters)
        shrink.most_available_node()
        self.assertIsNone(shrink.shrink_node_name)


class TestActionShrink_route_index(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
        self.shrink = Shrink(self.ilo)

    def test_raises(self):
        self.builder()
        self.client.indices.put_settings.side_effect = testvars.fake_fail
        self.assertRaises(
            Exception,
            self.shrink.route_index,
            'index',
            'exclude',
            '_name',
            'not_my_node',
        )


class TestActionShrink_dry_run(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.node_name = 'node_name'
        self.node_id = 'my_node'
        self.byte_count = 123456
        self.client.nodes.stats.return_value = {
            'nodes': {
                self.node_id: {
                    'fs': {
                        'data': ['one'],
                        'total': {'available_in_bytes': self.byte_count},
                    },
                    'name': self.node_name,
                }
            }
        }
        self.ilo = IndexList(self.client)

    def test_dry_run(self):
        self.builder()
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'roles': ['data'], 'name': self.node_name}}
        }
        self.client.indices.get.return_value = {
            testvars.named_index: {'settings': {'index': {'number_of_shards': 2}}}
        }
        shrink = Shrink(
            self.ilo,
            shrink_node=self.node_name,
            post_allocation={
                'allocation_type': 'require',
                'key': '_name',
                'value': self.node_name,
            },
        )
        self.assertIsNone(shrink.do_dry_run())

    def test_dry_run_raises(self):
        self.builder()
        self.client.nodes.info.return_value = {
            'nodes': {self.node_id: {'roles': ['data'], 'name': self.node_name}}
        }
        self.client.indices.get.side_effect = testvars.fake_fail
        shrink = Shrink(self.ilo, shrink_node=self.node_name)
        self.assertRaises(Exception, shrink.do_dry_run)


class TestActionShrink_various(TestCase):
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = {'version': {'number': '8.0.0'}}
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.exists_alias.return_value = False
        self.node_name = 'node_name'
        self.node_id = 'my_node'
        self.byte_count = 1239132959
        self.client.nodes.stats.return_value = {
            'nodes': {
                self.node_id: {
                    'fs': {
                        'data': ['one'],
                        'total': {'available_in_bytes': self.byte_count},
                    },
                    'name': self.node_name,
                }
            }
        }
        self.ilo = IndexList(self.client)

    def test_target_exists(self):
        self.builder()
        self.client.indices.exists.return_value = True
        shrink = Shrink(self.ilo)
        self.assertRaises(
            ActionError, shrink._check_target_exists, testvars.named_index
        )

    def test_doc_count(self):
        self.builder()
        too_many = 2147483520
        self.client.indices.stats.return_value = {
            'indices': {
                testvars.named_index: {'primaries': {'docs': {'count': too_many}}}
            }
        }
        shrink = Shrink(self.ilo)
        self.assertRaises(ActionError, shrink._check_doc_count, testvars.named_index)

    def test_shard_count(self):
        self.builder()
        src_shards = 2
        shrink = Shrink(self.ilo, number_of_shards=src_shards)
        self.assertRaises(
            ActionError, shrink._check_shard_count, testvars.named_index, src_shards
        )

    def test_shard_factor(self):
        self.builder()
        src_shards = 5
        shrink = Shrink(self.ilo, number_of_shards=3)
        self.assertRaises(
            ActionError, shrink._check_shard_factor, testvars.named_index, src_shards
        )

    def test_check_all_shards(self):
        self.builder()
        self.client.cluster.state.return_value = {
            'routing_table': {
                'indices': {
                    testvars.named_index: {
                        'shards': {
                            '0': [
                                {
                                    'index': testvars.named_index,
                                    'node': 'not_this_node',
                                    'primary': True,
                                    'shard': 0,
                                    'state': 'STARTED',
                                }
                            ]
                        }
                    }
                }
            }
        }
        shrink = Shrink(self.ilo, shrink_node=self.node_name)
        shrink.shrink_node_id = self.node_id
        self.assertRaises(ActionError, shrink._check_all_shards, testvars.named_index)
