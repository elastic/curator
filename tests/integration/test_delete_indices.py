"""Test the Delete Indices action"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
import requests
from curator.helpers.getters import get_indices
from . import CuratorTestCase
from . import testvars

HOST = os.environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
# '      - filtertype: {0}\n'
# '        source: {1}\n'
# '        direction: {2}\n'
# '        timestring: {3}\n'
# '        unit: {4}\n'
# '        unit_count: {5}\n'
# '        field: {6}\n'
# '        stats_result: {7}\n'
# '        epoch: {8}\n')

ILM_KEYS = ['ilm-history-1-000001', 'ilm-history-1']


def exclude_ilm_history(index_list):
    """Remove any values from ILM_KEYS found in index_list"""
    for val in ILM_KEYS:
        if val in index_list:
            index_list.remove(val)
    return index_list


class TestActionFileDeleteIndices(CuratorTestCase):
    def test_retention_from_name_days(self):
        # Test extraction of unit_count from index name
        # Create indices for 10 days with retention time of 5 days in index name
        # Expected: 5 oldest indices are deleted, 5 remain
        self.args['prefix'] = 'logstash_5_'
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_pattern_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 30, '_([0-9]+)_'
            ),
        )
        self.invoke_runner()
        self.assertEqual(5, len(exclude_ilm_history(get_indices(self.client))))

    def test_retention_from_name_days_ignore_failed_match(self):
        # Test extraction of unit_count from index name
        # Create indices for 10 days with retention time of 5 days in index name
        # Create indices for 10 days with no retention time in index name
        # Expected: 5 oldest indices are deleted, 5 remain - 10 indices without
        # retention time are ignored and remain
        self.args['prefix'] = 'logstash_5_'
        self.create_indices(10)
        self.args['prefix'] = 'logstash_'
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_pattern_proto.format(
                'age',
                'name',
                'older',
                '\'%Y.%m.%d\'',
                'days',
                30,
                '_([0-9]+)_',
                ' ',
                ' ',
                ' ',
            ),
        )
        self.invoke_runner()
        self.assertEqual(15, len(exclude_ilm_history(get_indices(self.client))))

    def test_retention_from_name_days_keep_exclude_false_after_failed_match(self):
        # Test extraction of unit_count from index name and confirm correct
        # behavior after a failed regex match with no fallback time - see gh issue 1206
        # Create indices for 30 days with retention time of 5 days in index name
        #
        # Create indices for 10 days with no retention time in index name
        # that alphabetically sort before the 10 with retention time
        #
        # Create indices for 10 days with no retention time in index name
        # that sort after the 10 with retention time

        # Expected: 45 oldest matching indices are deleted, 5 matching indices remain
        # 20 indices without retention time are ignored and remain
        # overall 25 indices should remain
        self.args['prefix'] = 'logstash-aanomatch-'
        self.create_indices(10)
        self.args['prefix'] = 'logstash-match-5-'
        self.create_indices(30)
        self.args['prefix'] = 'logstash-zznomatch-'
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_pattern_proto.format(
                'age',
                'name',
                'older',
                '\'%Y.%m.%d\'',
                'days',
                -1,
                r'logstash-\w+-([0-9]+)-[0-9]{4}\.[0-9]{2}\.[0-9]{2}',
                ' ',
                ' ',
                ' ',
            ),
        )
        self.invoke_runner()
        self.assertEqual(25, len(exclude_ilm_history(get_indices(self.client))))

    def test_retention_from_name_days_failed_match_with_fallback(self):
        # Test extraction of unit_count from index name
        # Create indices for 10 days with retention time of 5 days in index name
        # Create indices for 10 days with no retention time in index name but
        # configure fallback value of 7
        # Expected: 5 oldest indices are deleted, 5 remain - 7 indices without
        # retention time are ignored and remain due to the fallback value
        self.args['prefix'] = 'logstash_5_'
        self.create_indices(10)
        self.args['prefix'] = 'logstash_'
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_pattern_proto.format(
                'age',
                'name',
                'older',
                '\'%Y.%m.%d\'',
                'days',
                7,
                '_([0-9]+)_',
                ' ',
                ' ',
                ' ',
            ),
        )
        self.invoke_runner()
        self.assertEqual(12, len(exclude_ilm_history(get_indices(self.client))))

    def test_retention_from_name_no_capture_group(self):
        # Test extraction of unit_count from index name when pattern contains no
        # capture group
        # Create indices for 10 months with retention time of 2 months in index name
        # Expected: all indices remain as the pattern cannot be used to extract a
        # retention time
        self.args['prefix'] = 'logstash_2_'
        self.args['time_unit'] = 'months'
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_pattern_proto.format(
                'age',
                'name',
                'older',
                '\'%Y.%m\'',
                'months',
                -1,
                '_[0-9]+_',
                ' ',
                ' ',
                ' ',
            ),
        )
        self.invoke_runner()
        self.assertEqual(10, len(exclude_ilm_history(get_indices(self.client))))

    def test_retention_from_name_illegal_regex_no_fallback(self):
        # Test extraction of unit_count from index name when pattern contains an
        # illegal regular expression
        # Create indices for 10 months with retention time of 2 months in index name
        # Expected: all indices remain as the pattern cannot be used to extract a
        # retention time
        self.args['prefix'] = 'logstash_2_'
        self.args['time_unit'] = 'months'
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_pattern_proto.format(
                'age',
                'name',
                'older',
                '\'%Y.%m\'',
                'months',
                -1,
                '_[0-9+_',
                ' ',
                ' ',
                ' ',
            ),
        )
        self.invoke_runner()
        self.assertEqual(10, len(exclude_ilm_history(get_indices(self.client))))

    def test_retention_from_name_illegal_regex_with_fallback(self):
        # Test extraction of unit_count from index name when pattern contains an
        # illegal regular expression
        # Create indices for 10 days with retention time of 2 days in index name
        # Expected: Fallback value of 3 is used and 3 most recent indices remain
        # in place
        self.args['prefix'] = 'logstash_2_'
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_pattern_proto.format(
                'age',
                'name',
                'older',
                '\'%Y.%m.%d\'',
                'days',
                3,
                '_[0-9+_',
                ' ',
                ' ',
                ' ',
            ),
        )
        self.invoke_runner()
        self.assertEqual(3, len(exclude_ilm_history(get_indices(self.client))))

    def test_name_older_than_now(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            ),
        )
        self.invoke_runner()
        self.assertEqual(5, len(exclude_ilm_history(get_indices(self.client))))

    def test_name_based_age_match_five(self):
        self.create_indices(10)
        self.create_index(name='foobert')
        self.create_index(name='.shouldbehidden')
        self.create_index(name='this_has_no_date')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            ),
        )
        self.invoke_runner()
        self.assertEqual(8, len(exclude_ilm_history(get_indices(self.client))))

    def test_creation_date_newer_than_epoch(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_proto.format(
                'age',
                'creation_date',
                'younger',
                ' ',
                'seconds',
                0,
                ' ',
                ' ',
                int(time.time()) - 60,
            ),
        )
        self.invoke_runner()
        self.assertEqual(0, len(exclude_ilm_history(get_indices(self.client))))

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
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_period_proto.format(
                'period',
                'name',
                '-5',
                '-1',
                "'%Y.%m.%d'",
                'days',
                ' ',
                ' ',
                ' ',
                ' ',
                'monday',
            ),
        )
        self.invoke_runner()
        self.assertEqual(0, self.result.exit_code)
        self.assertEqual(5, len(exclude_ilm_history(get_indices(self.client))))

    def test_delete_in_period_absolute_date(self):
        delete_period_abs = (
            '---\n'
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
            '      date_to_format: {7}\n'
        )
        expected = 'index-2017.02.02'
        self.create_index('index-2017.01.02')
        self.create_index('index-2017.01.03')
        self.create_index(expected)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            delete_period_abs.format(
                'period',
                'name',
                '2017.01.01',
                '2017.01.10',
                "'%Y.%m.%d'",
                'days',
                "'%Y.%m.%d'",
                "'%Y.%m.%d'",
            ),
        )
        self.invoke_runner()
        indices = exclude_ilm_history(get_indices(self.client))
        self.assertEqual(0, self.result.exit_code)
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
        idx1, idx2 = ('intersecting', 'notintersecting')
        self.create_index(idx1)
        self.create_index(idx2)
        self.client.index(
            index=idx1,
            id='1',
            document={'@timestamp': '2017-09-25T01:00:00Z', 'doc': 'Earliest'},
        )
        self.client.index(
            index=idx1,
            id='2',
            document={'@timestamp': '2017-09-29T01:00:00Z', 'doc': 'Latest'},
        )
        self.client.index(
            index=idx2,
            id='1',
            document={'@timestamp': '2017-09-01T01:00:00Z', 'doc': 'Earliest'},
        )
        self.client.index(
            index=idx2,
            id='2',
            document={'@timestamp': '2017-09-29T01:00:00Z', 'doc': 'Latest'},
        )
        # Decorators cause this pylint error
        # pylint: disable=E1123
        self.client.indices.flush(index='*', force=True)
        self.client.indices.refresh(index=f'{idx1},{idx2}')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_period_proto.format(
                'period',
                'field_stats',
                '0',
                '0',
                ' ',
                'weeks',
                "'@timestamp'",
                'min_value',
                'true',
                1506716040,
                'sunday',
            ),
        )
        self.invoke_runner()
        self.assertEqual(0, self.result.exit_code)
        indices = exclude_ilm_history(get_indices(self.client))
        self.assertEqual(1, len(indices))
        self.assertEqual(idx2, indices[0])

    def test_empty_list(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_proto.format(
                'age',
                'creation_date',
                'older',
                ' ',
                'days',
                90,
                ' ',
                ' ',
                int(time.time()),
            ),
        )
        self.invoke_runner()
        self.assertEqual(10, len(exclude_ilm_history(get_indices(self.client))))
        self.assertEqual(1, self.result.exit_code)

    def test_ignore_empty_list(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_ignore_proto.format(
                'age',
                'creation_date',
                'older',
                ' ',
                'days',
                90,
                ' ',
                ' ',
                int(time.time()),
            ),
        )
        self.invoke_runner()
        self.assertEqual(10, len(exclude_ilm_history(get_indices(self.client))))
        self.assertEqual(0, self.result.exit_code)

    def test_extra_options(self):
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.bad_option_proto_test.format('delete_indices'),
        )
        self.invoke_runner()
        self.assertEqual(1, self.result.exit_code)

    def test_945(self):
        self.create_indices(10)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(self.args['actionfile'], testvars.test_945)
        self.invoke_runner()
        self.assertEqual(1, self.result.exit_code)

    def test_name_epoch_zero(self):
        self.create_index('epoch_zero-1970.01.01')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            ),
        )
        self.invoke_runner()
        self.assertEqual(0, len(exclude_ilm_history(get_indices(self.client))))

    def test_name_negative_epoch(self):
        self.create_index('index-1969.12.31')
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            ),
        )
        self.invoke_runner()
        self.assertEqual(0, len(exclude_ilm_history(get_indices(self.client))))

    def test_allow_ilm_indices_true(self):
        name = 'test'
        policy = {
            'policy': {
                'phases': {
                    'hot': {
                        'min_age': '0ms',
                        'actions': {'rollover': {'max_age': '2h', 'max_docs': 4}},
                    }
                }
            }
        }
        url = f'{HOST}/_ilm/policy/{name}'
        _ = requests.put(url, json=policy, timeout=30)
        # print(r.text) # logging reminder
        self.create_indices(10, ilm_policy=name)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.ilm_delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' ', 'true'
            ),
        )
        self.invoke_runner()
        self.assertEqual(5, len(exclude_ilm_history(get_indices(self.client))))

    def test_allow_ilm_indices_false(self):
        name = 'test'
        policy = {
            'policy': {
                'phases': {
                    'hot': {
                        'min_age': '0ms',
                        'actions': {'rollover': {'max_age': '2h', 'max_docs': 4}},
                    }
                }
            }
        }
        url = f'{HOST}/_ilm/policy/{name}'
        _ = requests.put(url, json=policy, timeout=30)
        # print(r.text) # logging reminder
        self.create_indices(10, ilm_policy=name)
        self.write_config(self.args['configfile'], testvars.client_config.format(HOST))
        self.write_config(
            self.args['actionfile'],
            testvars.ilm_delete_proto.format(
                'age',
                'name',
                'older',
                '\'%Y.%m.%d\'',
                'days',
                5,
                ' ',
                ' ',
                ' ',
                'false',
            ),
        )
        self.invoke_runner()
        self.assertEqual(10, len(exclude_ilm_history(get_indices(self.client))))


class TestCLIDeleteIndices(CuratorTestCase):
    def test_name_older_than_now_cli(self):
        self.create_indices(10)
        args = self.get_runner_args()
        args += [
            '--config',
            self.args['configfile'],
            'delete-indices',
            '--filter_list',
            (
                '{"filtertype":"age","source":"name","direction":"older",'
                '"timestring":"%Y.%m.%d","unit":"days","unit_count":5}'
            ),
        ]
        self.assertEqual(
            0,
            self.run_subprocess(
                args, logname='TestCLIDeleteIndices.test_name_older_than_now_cli'
            ),
        )
        self.assertEqual(5, len(exclude_ilm_history(get_indices(self.client))))
