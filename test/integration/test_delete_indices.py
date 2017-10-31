import elasticsearch
import curator
import os
import json
import string, random, tempfile
import time
from click import testing as clicktest
from mock import patch, Mock
import unittest
from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200
# '      - filtertype: {0}\n'
# '        source: {1}\n'
# '        direction: {2}\n'
# '        timestring: {3}\n'
# '        unit: {4}\n'
# '        unit_count: {5}\n'
# '        field: {6}\n'
# '        stats_result: {7}\n'
# '        epoch: {8}\n')

global_client = elasticsearch.Elasticsearch(host=host, port=port)

class TestCLIDeleteIndices(CuratorTestCase):
    def test_retention_from_name_days(self):
        # Test extraction of unit_count from index name
        # Create indices for 10 days with retention time of 5 days in index name
        # Expected: 5 oldest indices are deleted, 5 remain
        self.args['prefix'] = 'logstash_5_'
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
                          testvars.delete_pattern_proto.format(
                              'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 30, '_([0-9]+)_', ' ', ' ', ' '
                          )
                          )
        test = clicktest.CliRunner()
        result = test.invoke(
            curator.cli,
            [
                '--config', self.args['configfile'],
                self.args['actionfile']
            ],
        )
        self.assertEquals(5, len(curator.get_indices(self.client)))
    def test_retention_from_name_days_ignore_failed_match(self):
        # Test extraction of unit_count from index name
        # Create indices for 10 days with retention time of 5 days in index name
        # Create indices for 10 days with no retention time in index name
        # Expected: 5 oldest indices are deleted, 5 remain - 10 indices without retention time are ignored and remain
        self.args['prefix'] = 'logstash_5_'
        self.create_indices(10)
        self.args['prefix'] = 'logstash_'
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
                          testvars.delete_pattern_proto.format(
                              'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 30, '_([0-9]+)_', ' ', ' ', ' '
                          )
                          )
        test = clicktest.CliRunner()
        result = test.invoke(
            curator.cli,
            [
                '--config', self.args['configfile'],
                self.args['actionfile']
            ],
        )
        self.assertEquals(15, len(curator.get_indices(self.client)))
    def test_retention_from_name_days_failed_match_with_fallback(self):
        # Test extraction of unit_count from index name
        # Create indices for 10 days with retention time of 5 days in index name
        # Create indices for 10 days with no retention time in index name but configure fallback value of 7
        # Expected: 5 oldest indices are deleted, 5 remain - 7 indices without retention time are ignored and remain due to the fallback value
        self.args['prefix'] = 'logstash_5_'
        self.create_indices(10)
        self.args['prefix'] = 'logstash_'
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
                          testvars.delete_pattern_proto.format(
                              'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 7, '_([0-9]+)_', ' ', ' ', ' '
                          )
                          )
        test = clicktest.CliRunner()
        result = test.invoke(
            curator.cli,
            [
                '--config', self.args['configfile'],
                self.args['actionfile']
            ],
        )
        self.assertEquals(12, len(curator.get_indices(self.client)))
    def test_retention_from_name_no_capture_group(self):
        # Test extraction of unit_count from index name when pattern contains no capture group
        # Create indices for 10 months with retention time of 2 months in index name
        # Expected: all indices remain as the pattern cannot be used to extract a retention time
        self.args['prefix'] = 'logstash_2_'
        self.args['time_unit'] = 'months'
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
                          testvars.delete_pattern_proto.format(
                              'age', 'name', 'older', '\'%Y.%m\'', 'months', -1, '_[0-9]+_', ' ', ' ', ' '
                          )
                          )
        test = clicktest.CliRunner()
        result = test.invoke(
            curator.cli,
            [
                '--config', self.args['configfile'],
                self.args['actionfile']
            ],
        )
        self.assertEquals(10, len(curator.get_indices(self.client)))
    def test_retention_from_name_illegal_regex_no_fallback(self):
        # Test extraction of unit_count from index name when pattern contains an illegal regular expression
        # Create indices for 10 months with retention time of 2 months in index name
        # Expected: all indices remain as the pattern cannot be used to extract a retention time
        self.args['prefix'] = 'logstash_2_'
        self.args['time_unit'] = 'months'
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
                          testvars.delete_pattern_proto.format(
                              'age', 'name', 'older', '\'%Y.%m\'', 'months', -1, '_[0-9+_', ' ', ' ', ' '
                          )
                          )
        test = clicktest.CliRunner()
        result = test.invoke(
            curator.cli,
            [
                '--config', self.args['configfile'],
                self.args['actionfile']
            ],
        )
        self.assertEquals(10, len(curator.get_indices(self.client)))
    def test_retention_from_name_illegal_regex_with_fallback(self):
        # Test extraction of unit_count from index name when pattern contains an illegal regular expression
        # Create indices for 10 days with retention time of 2 days in index name
        # Expected: Fallback value of 3 is used and 3 most recent indices remain in place
        self.args['prefix'] = 'logstash_2_'
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
                          testvars.delete_pattern_proto.format(
                              'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 3, '_[0-9+_', ' ', ' ', ' '
                          )
                          )
        test = clicktest.CliRunner()
        result = test.invoke(
            curator.cli,
            [
                '--config', self.args['configfile'],
                self.args['actionfile']
            ],
        )
        self.assertEquals(3, len(curator.get_indices(self.client)))
    def test_name_older_than_now(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(5, len(curator.get_indices(self.client)))
    def test_creation_date_newer_than_epoch(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'creation_date', 'younger', ' ', 'seconds', 0,
                ' ', ' ', int(time.time()) - 60
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(0, len(curator.get_indices(self.client)))
    def test_delete_in_period(self):
        # filtertype: {0}
        # source: {1}
        # range_from: {2}
        # range_to: {3}
        # timestring: {4}
        # unit: {5}
        # field: {6}
        # stats_result: {7}
        # intersect: {8}
        # epoch: {9}
        # week_starts_on: {10}
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_period_proto.format(
                'period', 'name', '-5', '-1', "'%Y.%m.%d'", 'days',
                ' ', ' ', ' ', ' ', 'monday'
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(0, result.exit_code)
        self.assertEquals(5, len(curator.get_indices(self.client)))
    def test_delete_in_period_absolute_date(self):
        delete_period_abs = ('---\n'
        'actions:\n'
        '  1:\n'
        '    description: "Delete indices as filtered"\n'
        '    action: delete_indices\n'
        '    options:\n'
        '      continue_if_exception: False\n'
        '      disable_action: False\n'
        '    filters:\n'
        '    - filtertype: {0}\n'
        '      period_type: absolute\n'
        '      source: {1}\n'
        '      date_from: {2}\n'
        '      date_to: {3}\n'
        '      timestring: {4}\n'
        '      unit: {5}\n'
        '      date_from_format: {6}\n'
        '      date_to_format: {7}\n')
        expected = 'index-2017.02.02'
        self.create_index('index-2017.01.02')
        self.create_index('index-2017.01.03')
        self.create_index(expected)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            delete_period_abs.format(
                'period', 'name', '2017.01.01', '2017.01.10', "'%Y.%m.%d'", 'days',
                "'%Y.%m.%d'", "'%Y.%m.%d'"
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        indices = curator.get_indices(self.client)
        self.assertEqual(0, result.exit_code)
        self.assertEqual(1, len(indices))
        self.assertEqual(expected, indices[0])
    def test_delete_in_period_intersect(self):
        # filtertype: {0}
        # source: {1}
        # range_from: {2}
        # range_to: {3}
        # timestring: {4}
        # unit: {5}
        # field: {6}
        # stats_result: {7}
        # intersect: {8}
        # epoch: {9}
        # week_starts_on: {10}
        # 2017-09-01T01:00:00 = 1504227600
        # 2017-09-25T01:00:00 = 1506301200
        # 2017-09-29T01:00:00 = 1506646800
        self.create_index('intersecting') 
        self.create_index('notintersecting')
        self.client.index(index='intersecting', doc_type='log', id='1', body={'@timestamp': '2017-09-25T01:00:00Z', 'doc' :'Earliest'})
        self.client.index(index='intersecting', doc_type='log', id='2', body={'@timestamp': '2017-09-29T01:00:00Z', 'doc' :'Latest'})
        self.client.index(index='notintersecting', doc_type='log', id='1', body={'@timestamp': '2017-09-01T01:00:00Z', 'doc' :'Earliest'})
        self.client.index(index='notintersecting', doc_type='log', id='2', body={'@timestamp': '2017-09-29T01:00:00Z', 'doc' :'Latest'})
        self.client.indices.flush(index='_all', force=True)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_period_proto.format(
                'period', 'field_stats', '0', '0', ' ', 'weeks',
                "'@timestamp'", 'min_value', 'true', 1506716040, 'sunday'
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(0, result.exit_code)
        indices = curator.get_indices(self.client)
        self.assertEquals(1, len(indices))
        self.assertEqual('notintersecting', indices[0])
    def test_empty_list(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'creation_date', 'older', ' ', 'days', 90,
                ' ', ' ', int(time.time())
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(10, len(curator.get_indices(self.client)))
        self.assertEqual(1, result.exit_code)
    def test_ignore_empty_list(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_ignore_proto.format(
                'age', 'creation_date', 'older', ' ', 'days', 90,
                ' ', ' ', int(time.time())
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(10, len(curator.get_indices(self.client)))
        self.assertEqual(0, result.exit_code)
    def test_extra_options(self):
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.bad_option_proto_test.format('delete_indices'))
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_945(self):
        self.create_indices(10)
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'], testvars.test_945)
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEqual(-1, result.exit_code)
    def test_name_epoch_zero(self):
        self.create_index('epoch_zero-1970.01.01')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(0, len(curator.get_indices(self.client)))
    def test_name_negative_epoch(self):
        self.create_index('index-1969.12.31')
        self.write_config(
            self.args['configfile'], testvars.client_config.format(host, port))
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            )
        )
        test = clicktest.CliRunner()
        result = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(0, len(curator.get_indices(self.client)))
