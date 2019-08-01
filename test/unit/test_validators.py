import sys
import logging
from unittest import TestCase
from mock import Mock, patch, mock_open
from voluptuous import *
import curator


def shared_result(config, action):
    return curator.validators.SchemaCheck(
        config,
        Schema(curator.validators.filters.Filters(action)),
        'filters',
        'testing'
    ).result()

class TestFilters(TestCase):
    def test_single_raises_configuration_error(self):
        data = {'max_num_segments': 1, 'exclude': True}
        self.assertRaises(
            curator.ConfigurationError,
            curator.validators.filters.single,
            'forcemerge',
            data
        )

class TestFilterTypes(TestCase):
    def test_alias(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'alias',
                'aliases' : ['alias1', 'alias2'],
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_age(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'age',
                'direction' : 'older',
                'unit' : 'days',
                'unit_count' : 1,
                'source' : 'field_stats',
                'field'  : '@timestamp',
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_age_with_string_unit_count(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'age',
                'direction' : 'older',
                'unit' : 'days',
                'unit_count' : "1",
                'source' : 'field_stats',
                'field'  : '@timestamp',
            }
        ]
        result = shared_result(config, action)
        self.assertEqual(1, result[0]['unit_count'])
    def test_allocated(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'allocated',
                'key' : 'foo',
                'value' : 'bar',
                'allocation_type' : 'require',
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_closed(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'closed',
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_count(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'count',
                'count' : 1,
                'reverse' : True,
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_forcemerged(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'forcemerged',
                'max_num_segments' : 1,
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_kibana(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'kibana',
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_opened(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'opened',
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_space_name_age(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'space',
                'disk_space' : 1,
                'use_age' : True,
                'exclude' : False,
                'source' : 'name',
                'timestring' : '%Y.%m.%d',
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_space_name_age_string_float(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'space',
                'disk_space' : "1.0",
                'use_age' : True,
                'exclude' : False,
                'source' : 'name',
                'timestring' : '%Y.%m.%d',
            }
        ]
        result = shared_result(config, action)
        self.assertEqual(1.0, result[0]['disk_space'])
    def test_space_name_age_no_ts(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'space',
                'disk_space' : 1,
                'use_age' : True,
                'exclude' : False,
                'source' : 'name',
            }
        ]
        schema = curator.validators.SchemaCheck(
            config,
            Schema(curator.validators.filters.Filters(action)),
            'filters',
            'testing'
        )
        self.assertRaises(curator.ConfigurationError, schema.result)
    def test_space_field_stats_age(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'space',
                'disk_space' : 1,
                'use_age' : True,
                'exclude' : False,
                'source' : 'field_stats',
                'field' : '@timestamp',
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_space_field_stats_age_no_field(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'space',
                'disk_space' : 1,
                'use_age' : True,
                'exclude' : False,
                'source' : 'field_stats',
            }
        ]
        schema = curator.validators.SchemaCheck(
            config,
            Schema(curator.validators.filters.Filters(action)),
            'filters',
            'testing'
        )
        self.assertRaises(curator.ConfigurationError, schema.result)
    def test_space_creation_date_age(self):
        action = 'delete_indices'
        config = [
            {
                'filtertype' : 'space',
                'disk_space' : 1,
                'use_age' : True,
                'exclude' : False,
                'source' : 'creation_date',
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_state(self):
        action = 'delete_snapshots'
        config = [
            {
                'filtertype' : 'state',
                'state' : 'SUCCESS',
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
    def test_shards(self):
        action = 'shrink'
        config = [
            {
                'filtertype' : 'shards',
                'number_of_shards' : 5,
                'shard_filter_behavior': 'greater_than',
                'exclude' : False,
            }
        ]
        self.assertEqual(config, shared_result(config, action))
