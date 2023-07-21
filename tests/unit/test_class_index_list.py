"""Test index_list class"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long, attribute-defined-outside-init, protected-access
from unittest import TestCase
from copy import deepcopy
from mock import Mock
import yaml
from curator.exceptions import ActionError, ConfigurationError, FailedExecution, MissingArgument, NoIndices
from curator.helpers.date_ops import fix_epoch
from curator import IndexList
# Get test variables and constants from a single source
from . import testvars

def get_es_ver():
    return {'version': {'number': '8.0.0'} }

def get_testvals(number, key):
    """Return the appropriate value per the provided key number"""
    data = {
        "1": {
            "settings": testvars.settings_one,
            "state": testvars.state_one,
            "stats": testvars.stats_one,
            "fieldstats": testvars.fieldstats_one,
        },
        "2": {
            "settings": testvars.settings_two,
            "state": testvars.state_two,
            "stats": testvars.stats_two,
            "fieldstats": testvars.fieldstats_two,
        },
        "4": {
            "settings": testvars.settings_four,
            "state": testvars.state_four,
            "stats": testvars.stats_four,
            "fieldstats": testvars.fieldstats_four,
        },
    }
    return data[number][key]

class TestIndexListClientAndInit(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_init_bad_client(self):
        client = 'not a real client'
        self.assertRaises(TypeError, IndexList, client)
    def test_init_get_indices_exception(self):
        self.builder()
        self.client.indices.get_settings.side_effect = testvars.fake_fail
        self.assertRaises(FailedExecution, IndexList, self.client)
    def test_init(self):
        self.builder()
        self.ilo.get_index_stats()
        self.ilo.get_index_settings()
        self.assertEqual(
            testvars.stats_two['indices']['index-2016.03.03']['total']['store']['size_in_bytes'],
            self.ilo.index_info['index-2016.03.03']['size_in_bytes']
        )
        self.assertEqual(
            testvars.state_two[1]['status'],
            self.ilo.index_info['index-2016.03.04']['state']
        )
        self.assertEqual(['index-2016.03.03','index-2016.03.04'], sorted(self.ilo.indices))
    def test_for_closed_index(self):
        self.builder()
        self.client.cat.indices.return_value = testvars.state_2_closed
        self.client.indices.get_settings.return_value = testvars.settings_2_closed
        ilo2 = IndexList(self.client)
        ilo2.get_index_state()
        self.assertEqual('close', ilo2.index_info['index-2016.03.03']['state'])

class TestIndexListOtherMethods(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_empty_list(self):
        self.builder()
        self.client.indices.exists_alias.return_value = False
        self.assertEqual(2, len(self.ilo.indices))
        self.ilo.indices = []
        self.assertRaises(NoIndices, self.ilo.empty_list_check)
    def test_get_segmentcount(self):
        self.builder(key='1')
        self.client.indices.segments.return_value = testvars.shards
        # Ordinarily get_index_state is run before get_segment_counts, so we do so manually here.
        self.ilo.get_index_state()
        self.ilo.get_segment_counts()
        self.assertEqual(71, self.ilo.index_info[testvars.named_index]['segments'])

class TestIndexListAgeFilterName(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_get_name_based_ages_match(self):
        self.builder()
        self.ilo.get_index_state()
        self.ilo.get_index_settings()
        self.ilo._get_name_based_ages('%Y.%m.%d')
        self.assertEqual(1456963200, self.ilo.index_info['index-2016.03.03']['age']['name'])
    def test_get_name_based_ages_no_match(self):
        self.builder()
        self.ilo.get_index_settings()
        self.ilo._get_name_based_ages('%Y-%m-%d')
        self.assertEqual(
            fix_epoch(
                testvars.settings_two['index-2016.03.03']['settings']['index']['creation_date']
            ),
            self.ilo.index_info['index-2016.03.03']['age']['creation_date']
        )

class TestIndexListAgeFilterStatsAPI(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_get_field_stats_dates_negative(self):
        self.builder()
        self.client.search.return_value = testvars.fieldstats_query
        self.client.field_stats.return_value = testvars.fieldstats_two
        self.ilo._get_field_stats_dates(field='timestamp')
        self.assertNotIn('not_an_index_name', list(self.ilo.index_info.keys()))
    def test_get_field_stats_dates_field_not_found(self):
        self.builder()
        self.client.search.return_value = {'aggregations': {'foo':'bar'}}
        self.assertRaises(
            ActionError, self.ilo._get_field_stats_dates, field='not_in_index')

class TestIndexListRegexFilters(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_filter_by_regex_prefix(self):
        self.builder()
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='prefix', value='ind')
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='prefix', value='ind', exclude=True)
        self.assertEqual([], self.ilo.indices)
    def test_filter_by_regex_middle(self):
        self.builder()
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='regex', value='dex')
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='regex', value='dex', exclude=True)
        self.assertEqual([], self.ilo.indices)
    def test_filter_by_regex_timestring(self):
        self.builder()
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='timestring', value='%Y.%m.%d')
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='timestring', value='%Y.%m.%d', exclude=True)
        self.assertEqual([], self.ilo.indices)
    def test_filter_by_regex_no_match_exclude(self):
        self.builder()
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='prefix', value='invalid', exclude=True)
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
    def test_filter_by_regex_no_value(self):
        self.builder()
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.assertRaises(ValueError, self.ilo.filter_by_regex, kind='prefix', value=None)
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.ilo.filter_by_regex(kind='prefix', value=0)
        self.assertEqual([], self.ilo.indices)
    def test_filter_by_regex_bad_kind(self):
        self.builder()
        self.assertEqual(['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
        self.assertRaises(ValueError, self.ilo.filter_by_regex, kind='invalid', value=None)

class TestIndexListFilterByAge(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_missing_direction(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_by_age, unit='days', unit_count=1)
    def test_bad_direction(self):
        self.builder()
        self.assertRaises(ValueError, self.ilo.filter_by_age, unit='days',
            unit_count=1, direction="invalid"
        )
    def test_name_no_timestring(self):
        self.builder()
        self.assertRaises(MissingArgument,
            self.ilo.filter_by_age,
            source='name', unit='days', unit_count=1, direction='older'
        )
    def test_name_older_than_now(self):
        self.builder()
        self.ilo.filter_by_age(source='name', direction='older',
            timestring='%Y.%m.%d', unit='days', unit_count=1
        )
        self.assertEqual(['index-2016.03.03','index-2016.03.04'], sorted(self.ilo.indices))
    def test_name_older_than_now_exclude(self):
        self.builder()
        self.ilo.filter_by_age(source='name', direction='older',
            timestring='%Y.%m.%d', unit='days', unit_count=1, exclude=True
        )
        self.assertEqual([], sorted(self.ilo.indices))
    def test_name_younger_than_now(self):
        self.builder()
        self.ilo.filter_by_age(source='name', direction='younger',
            timestring='%Y.%m.%d', unit='days', unit_count=1
        )
        self.assertEqual([], sorted(self.ilo.indices))
    def test_name_younger_than_now_exclude(self):
        self.builder()
        self.ilo.filter_by_age(source='name', direction='younger',
            timestring='%Y.%m.%d', unit='days', unit_count=1, exclude=True
        )
        self.assertEqual(['index-2016.03.03','index-2016.03.04'], sorted(self.ilo.indices))
    def test_name_younger_than_past_date(self):
        self.builder()
        self.ilo.filter_by_age(source='name', direction='younger',
            timestring='%Y.%m.%d', unit='seconds', unit_count=0,
            epoch=1457049599
        )
        self.assertEqual(['index-2016.03.04'], sorted(self.ilo.indices))
    def test_name_older_than_past_date(self):
        self.builder()
        self.ilo.filter_by_age(source='name', direction='older',
            timestring='%Y.%m.%d', unit='seconds', unit_count=0,
            epoch=1456963201
        )
        self.assertEqual(['index-2016.03.03'], sorted(self.ilo.indices))
    def test_creation_date_older_than_now(self):
        self.builder()
        self.ilo.filter_by_age(source='creation_date', direction='older', unit='days',
            unit_count=1
        )
        self.assertEqual(['index-2016.03.03','index-2016.03.04'], sorted(self.ilo.indices))
    def test_creation_date_older_than_now_raises(self):
        self.builder()
        self.ilo.get_index_state()
        self.ilo.get_index_settings()
        self.ilo.index_info['index-2016.03.03']['age'].pop('creation_date')
        self.ilo.index_info['index-2016.03.04']['age'].pop('creation_date')
        self.ilo.filter_by_age(
            source='creation_date', direction='older', unit='days', unit_count=1
        )
        self.assertEqual([], self.ilo.indices)
    def test_creation_date_younger_than_now(self):
        self.builder()
        self.ilo.filter_by_age(source='creation_date', direction='younger',
            unit='days', unit_count=1
        )
        self.assertEqual([], sorted(self.ilo.indices))
    def test_creation_date_younger_than_now_raises(self):
        self.builder()
        self.ilo.get_index_state()
        self.ilo.get_index_settings()
        self.ilo.index_info['index-2016.03.03']['age'].pop('creation_date')
        self.ilo.index_info['index-2016.03.04']['age'].pop('creation_date')
        self.ilo.filter_by_age(
            source='creation_date', direction='younger', unit='days',
            unit_count=1
        )
        self.assertEqual([], self.ilo.indices)
    def test_creation_date_younger_than_past_date(self):
        self.builder()
        self.ilo.filter_by_age(source='creation_date', direction='younger',
            unit='seconds', unit_count=0, epoch=1457049599
        )
        self.assertEqual(['index-2016.03.04'], sorted(self.ilo.indices))
    def test_creation_date_older_than_past_date(self):
        self.builder()
        self.ilo.filter_by_age(source='creation_date', direction='older',
            unit='seconds', unit_count=0, epoch=1456963201
        )
        self.assertEqual(['index-2016.03.03'], sorted(self.ilo.indices))
    def test_field_stats_missing_field(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_by_age,
            source='field_stats', direction='older', unit='days', unit_count=1
        )
    def test_field_stats_invalid_stats_result(self):
        self.builder()
        self.assertRaises(ValueError, self.ilo.filter_by_age, field='timestamp',
            source='field_stats', direction='older', unit='days', unit_count=1,
            stats_result='invalid'
        )
    def test_field_stats_invalid_source(self):
        self.builder()
        self.assertRaises(ValueError, self.ilo.filter_by_age,
            source='invalid', direction='older', unit='days', unit_count=1
        )

class TestIndexListFilterBySpace(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.search.return_value = get_testvals(key, 'fieldstats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_missing_disk_space_value(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_by_space)
    def test_filter_result_by_name(self):
        self.builder()
        self.ilo.filter_by_space(disk_space=1.1)
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
    def test_filter_result_by_name_reverse_order(self):
        self.builder()
        self.ilo.filter_by_space(disk_space=1.1, reverse=False)
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_filter_result_by_name_exclude(self):
        self.builder()
        self.ilo.filter_by_space(disk_space=1.1, exclude=True)
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_filter_result_by_date_raise(self):
        self.builder(key='4')
        self.assertRaises(ValueError,
            self.ilo.filter_by_space, disk_space=2.1, use_age=True, source='invalid'
        )
    def test_filter_result_by_date_timestring_raise(self):
        self.builder(key='4')
        self.assertRaises(MissingArgument,
            self.ilo.filter_by_space, disk_space=2.1, use_age=True, source='name'
        )
    def test_filter_result_by_date_timestring(self):
        self.builder(key='4')
        self.ilo.filter_by_space(
            disk_space=2.1, use_age=True,
            source='name', timestring='%Y.%m.%d'
        )
        self.assertEqual(['a-2016.03.03'], sorted(self.ilo.indices))
    def test_filter_result_by_date_non_matching_timestring(self):
        self.builder(key='4')
        self.ilo.filter_by_space(
            disk_space=2.1, use_age=True,
            source='name', timestring='%Y.%m.%d.%H'
        )
        self.assertEqual([], sorted(self.ilo.indices))
    def test_filter_threshold_behavior(self):
        self.builder()
        il_a = IndexList(self.client)
        # less than
        il_a.filter_by_space(
            disk_space=1.5, use_age=True,
            threshold_behavior='less_than'
        )
        self.assertEqual(['index-2016.03.04'], sorted(il_a.indices))
        # greater than
        il_b = IndexList(self.client)
        il_b.filter_by_space(
            disk_space=1.5, use_age=True,
            threshold_behavior='greater_than'
        )
        self.assertEqual(['index-2016.03.03'], sorted(il_b.indices))
        # default case
        il_c = IndexList(self.client)
        il_c.filter_by_space(
            disk_space=1.5, use_age=True
        )
        self.assertEqual(['index-2016.03.03'], sorted(il_c.indices))
    def test_filter_bad_threshold_behavior(self):
        self.builder()
        # less than
        self.assertRaises(
            ValueError,
            self.ilo.filter_by_space, disk_space=1.5, threshold_behavior='invalid'
        )
    def test_filter_result_by_date_field_stats_raise(self):
        self.builder(key='4')
        self.client.search.return_value = testvars.fieldstats_query
        self.assertRaises(ValueError,
            self.ilo.filter_by_space, disk_space=2.1, use_age=True,
            source='min_value'
        )
    def test_filter_result_by_date_no_field_raise(self):
        self.builder(key='4')
        self.assertRaises(MissingArgument,
            self.ilo.filter_by_space, disk_space=2.1, use_age=True,
            source='field_stats'
        )
    def test_filter_result_by_date_invalid_stats_result_raise(self):
        self.builder(key='4')
        self.assertRaises(ValueError,
            self.ilo.filter_by_space, disk_space=2.1, use_age=True,
            source='field_stats', field='timestamp', stats_result='invalid'
        )
    def test_filter_result_by_creation_date(self):
        self.builder(key='4')
        self.ilo.filter_by_space(disk_space=2.1, use_age=True)
        self.assertEqual(['a-2016.03.03'], self.ilo.indices)

class TestIndexListFilterKibana(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.search.return_value = get_testvals(key, 'fieldstats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_filter_kibana_positive(self):
        self.builder()
        # Establish the object per requirements, then overwrite
        self.ilo.indices = ['.kibana', '.kibana-5', '.kibana-6', 'dummy']
        self.ilo.filter_kibana()
        self.assertEqual(['dummy'], self.ilo.indices)
    def test_filter_kibana_positive_include(self):
        self.builder()
        # Establish the object per requirements, then overwrite
        self.ilo.indices = ['.kibana', '.kibana-5', '.kibana-6', 'dummy']
        self.ilo.filter_kibana(exclude=False)
        self.assertEqual(['.kibana', '.kibana-5', '.kibana-6'], self.ilo.indices)
    def test_filter_kibana_positive_exclude(self):
        self.builder()
        # Establish the object per requirements, then overwrite
        kibana_indices = ['.kibana', '.kibana-5', '.kibana-6']
        self.ilo.indices = kibana_indices
        self.ilo.indices.append('dummy')
        self.ilo.filter_kibana(exclude=True)
        self.assertEqual(kibana_indices, self.ilo.indices)
    def test_filter_kibana_negative(self):
        self.builder()
        # Establish the object per requirements, then overwrite
        self.ilo.indices = ['kibana', 'marvel-kibana', 'cabana-int', 'marvel-es-data', 'dummy']
        self.ilo.filter_kibana()
        self.assertEqual(
            ['kibana', 'marvel-kibana', 'cabana-int', 'marvel-es-data', 'dummy'],
             self.ilo.indices
        )

class TestIndexListFilterForceMerged(TestCase):
    VERSION = {'version': {'number': '8.0.0'} }
    def builder(self):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = testvars.state_one
        self.client.indices.get_settings.return_value = testvars.settings_one
        self.client.indices.stats.return_value = testvars.stats_one
        self.client.indices.segments.return_value = testvars.shards
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_filter_forcemerge_raise(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_forceMerged)
    def test_filter_forcemerge_positive(self):
        self.builder()
        self.ilo.filter_forceMerged(max_num_segments=2)
        self.assertEqual([testvars.named_index], self.ilo.indices)
    def test_filter_forcemerge_negative(self):
        self.builder()
        self.client.indices.segments.return_value = testvars.fm_shards
        self.ilo.filter_forceMerged(max_num_segments=2)
        self.assertEqual([], self.ilo.indices)

class TestIndexListFilterOpened(TestCase):
    def test_filter_opened(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '8.0.0'} }
        client.cat.indices.return_value = testvars.state_four
        client.indices.get_settings.return_value = testvars.settings_four
        client.indices.stats.return_value = testvars.stats_four
        client.field_stats.return_value = testvars.fieldstats_four
        client.indices.exists_alias.return_value = False
        ilo = IndexList(client)
        ilo.filter_opened()
        self.assertEqual(['c-2016.03.05'], ilo.indices)

class TestIndexListFilterAllocated(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_missing_key(self):
        self.builder()
        self.assertRaises(
            MissingArgument, self.ilo.filter_allocated, value='foo',
            allocation_type='invalid'
        )
    def test_missing_value(self):
        self.builder()
        self.assertRaises(
            MissingArgument, self.ilo.filter_allocated, key='tag',
            allocation_type='invalid'
        )
    def test_invalid_allocation_type(self):
        self.builder()
        self.assertRaises(
            ValueError, self.ilo.filter_allocated, key='tag', value='foo',
            allocation_type='invalid'
        )
    def test_success(self):
        self.builder()
        self.ilo.filter_allocated(key='tag', value='foo', allocation_type='include')
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_invalid_tag(self):
        self.builder()
        self.ilo.filter_allocated(
            key='invalid', value='foo', allocation_type='include')
        self.assertEqual(
            ['index-2016.03.03','index-2016.03.04'], sorted(self.ilo.indices))

class TestIterateFiltersIndex(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_no_filters(self):
        self.builder(key='4')
        self.ilo.iterate_filters({})
        self.assertEqual(
            ['a-2016.03.03', 'b-2016.03.04', 'c-2016.03.05', 'd-2016.03.06'],
            sorted(self.ilo.indices)
        )
    def test_no_filtertype(self):
        self.builder(key='4')
        config = {'filters': [{'no_filtertype':'fail'}]}
        self.assertRaises(
            ConfigurationError, self.ilo.iterate_filters, config)
    def test_invalid_filtertype(self):
        self.builder(key='4')
        config = {'filters': [{'filtertype':12345.6789}]}
        self.assertRaises(
            ConfigurationError, self.ilo.iterate_filters, config)
    def test_pattern_filtertype(self):
        self.builder(key='4')
        config = yaml.load(testvars.pattern_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(['a-2016.03.03'], self.ilo.indices)
    def test_age_filtertype(self):
        self.builder()
        config = yaml.load(testvars.age_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
    def test_space_filtertype(self):
        self.builder(key='4')
        self.client.field_stats.return_value = testvars.fieldstats_four
        config = yaml.load(testvars.space_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(['a-2016.03.03'], self.ilo.indices)
    def test_forcemerge_filtertype(self):
        self.builder(key='1')
        self.client.indices.segments.return_value = testvars.shards
        config = yaml.load(testvars.forcemerge_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual([testvars.named_index], self.ilo.indices)
    def test_allocated_filtertype(self):
        self.builder()
        config = yaml.load(testvars.allocated_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_kibana_filtertype(self):
        self.builder()
        self.client.field_stats.return_value = testvars.fieldstats_two
        # Establish the object per requirements, then overwrite
        self.ilo.indices = ['.kibana', '.kibana-5', '.kibana-6', 'dummy']
        config = yaml.load(testvars.kibana_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(['dummy'], self.ilo.indices)
    def test_opened_filtertype(self):
        self.builder(key='4')
        self.client.field_stats.return_value = testvars.fieldstats_four
        config = yaml.load(testvars.opened_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(['c-2016.03.05'], self.ilo.indices)
    def test_closed_filtertype(self):
        self.builder(key='4')
        self.client.field_stats.return_value = testvars.fieldstats_four
        config = yaml.load(testvars.closed_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(
            ['a-2016.03.03','b-2016.03.04','d-2016.03.06'], sorted(self.ilo.indices))
    def test_none_filtertype(self):
        self.builder()
        config = yaml.load(testvars.none_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(
            ['index-2016.03.03', 'index-2016.03.04'], sorted(self.ilo.indices))
    def test_unknown_filtertype_raises(self):
        self.builder()
        config = yaml.load(testvars.invalid_ft, Loader=yaml.FullLoader)['actions'][1]
        self.assertRaises(ConfigurationError, self.ilo.iterate_filters, config)
    def test_ilm_filtertype_exclude(self):
        self.builder()
        # If we don't deepcopy, then it munges the settings for future references.
        with_ilm = deepcopy(testvars.settings_two)
        with_ilm['index-2016.03.03']['settings']['index']['lifecycle'] = {'name':'mypolicy'}
        self.client.indices.get_settings.return_value = with_ilm
        config = {'filters': [{'filtertype':'ilm','exclude':True}]}
        self.ilo.iterate_filters(config)
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_ilm_filtertype_no_setting(self):
        self.builder()
        config = {'filters': [{'filtertype':'ilm','exclude':True}]}
        self.ilo.iterate_filters(config)
        self.assertEqual(['index-2016.03.03','index-2016.03.04'], sorted(self.ilo.indices))
    def test_size_filtertype(self):
        self.builder()
        config = yaml.load(testvars.size_ft, Loader=yaml.FullLoader)['actions'][1]
        self.ilo.iterate_filters(config)
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)

class TestIndexListFilterAlias(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_raise(self):
        self.builder(key='1')
        self.assertRaises(MissingArgument, self.ilo.filter_by_alias)
    def test_positive(self):
        self.builder()
        self.client.indices.get_alias.return_value = testvars.settings_2_get_aliases
        self.ilo.filter_by_alias(aliases=['my_alias'])
        self.assertEqual(sorted(list(testvars.settings_two.keys())), sorted(self.ilo.indices))
    def test_negative(self):
        self.builder()
        self.client.indices.get_alias.return_value = {}
        self.ilo.filter_by_alias(aliases=['not_my_alias'])
        self.assertEqual(sorted([]), sorted(self.ilo.indices))
    def test_get_alias_raises(self):
        self.builder()
        self.client.indices.get_alias.side_effect = testvars.get_alias_fail
        self.client.indices.get_alias.return_value = testvars.settings_2_get_aliases
        self.ilo.filter_by_alias(aliases=['my_alias'])
        self.assertEqual(sorted([]), sorted(self.ilo.indices))

class TestIndexListFilterCount(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.client.indices.get_alias.return_value = testvars.settings_2_get_aliases
        self.ilo = IndexList(self.client)
    def test_raise(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_by_count)
    def test_without_age(self):
        self.builder()
        self.ilo.filter_by_count(count=1)
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
    def test_without_age_reversed(self):
        self.builder()
        self.ilo.filter_by_count(count=1, reverse=False)
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_with_age(self):
        self.builder()
        self.ilo.filter_by_count(count=1, use_age=True, source='name', timestring='%Y.%m.%d')
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
    def test_with_age_creation_date(self):
        self.builder()
        self.ilo.filter_by_count(count=1, use_age=True)
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
    def test_with_age_reversed(self):
        self.builder()
        self.ilo.filter_by_count(
            count=1, use_age=True, source='name', timestring='%Y.%m.%d',
            reverse=False
        )
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_pattern_no_regex_group(self):
        self.builder()
        self.assertRaises(ActionError, self.ilo.filter_by_count,
            count=1, use_age=True, pattern=' ', source='name', timestring='%Y.%m.%d',
        )
    def test_pattern_multiple_regex_groups(self):
        self.builder()
        self.assertRaises(ActionError, self.ilo.filter_by_count,
            count=1, use_age=True, pattern=r'^(\ )foo(\ )$', source='name', timestring='%Y.%m.%d',
        )

class TestIndexListFilterShards(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_filter_shards_raise(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_by_shards)
    def test_bad_shard_count_raise_1(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_by_shards, number_of_shards=0)
    def test_bad_shard_count_raise_2(self):
        self.builder()
        self.assertRaises(ValueError, self.ilo.filter_by_shards, number_of_shards=1, shard_filter_behavior='less_than')
    def test_bad_shard_count_raise_3(self):
        self.builder()
        self.assertRaises(ValueError, self.ilo.filter_by_shards, number_of_shards=-1, shard_filter_behavior='greater_than')
    def test_greater_than_or_equal(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, shard_filter_behavior='greater_than_or_equal')
        self.assertEqual(sorted(['index-2016.03.03', 'index-2016.03.04']), sorted(self.ilo.indices))
    def test_greater_than_or_equal_exclude(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, shard_filter_behavior='greater_than_or_equal', exclude=True)
        self.assertEqual(sorted([]), sorted(self.ilo.indices))
    def test_greater_than(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5)
        self.assertEqual(sorted([]), sorted(self.ilo.indices))
    def test_greater_than_exclude(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, exclude=True)
        self.assertEqual(sorted(['index-2016.03.03', 'index-2016.03.04']), sorted(self.ilo.indices))
    def test_less_than_or_equal(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, shard_filter_behavior='less_than_or_equal')
        self.assertEqual(sorted(['index-2016.03.03', 'index-2016.03.04']), sorted(self.ilo.indices))
    def test_less_than_or_equal_exclude(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, shard_filter_behavior='less_than_or_equal', exclude=True)
        self.assertEqual(sorted([]), sorted(self.ilo.indices))
    def test_less_than(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, shard_filter_behavior='less_than')
        self.assertEqual(sorted([]), sorted(self.ilo.indices))
    def test_less_than_exclude(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, shard_filter_behavior='less_than', exclude=True)
        self.assertEqual(sorted(['index-2016.03.03', 'index-2016.03.04']), sorted(self.ilo.indices))
    def test_equal(self):
        self.builder()
        self.ilo.filter_by_shards(number_of_shards=5, shard_filter_behavior='equal')
        self.assertEqual(sorted(['index-2016.03.03', 'index-2016.03.04']), sorted(self.ilo.indices))

class TestIndexListPeriodFilterName(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
        self.timestring = '%Y.%m.%d'
        self.epoch = 1456963201
        self.unit = 'days'
    def test_get_name_based_age_in_range(self):
        range_from = -1
        range_to = 0
        expected = ['index-2016.03.03']
        self.builder()
        self.ilo.filter_period(unit=self.unit, range_from=range_from, range_to=range_to,
            source='name', timestring=self.timestring, epoch=self.epoch)
        self.assertEqual(expected, self.ilo.indices)
    def test_get_name_based_age_not_in_range(self):
        range_from = -3
        range_to = -2
        expected = []
        self.builder()
        self.ilo.filter_period(unit=self.unit, range_from=range_from, range_to=range_to,
            source='name', timestring=self.timestring, epoch=self.epoch)
        self.assertEqual(expected, self.ilo.indices)
    def test_bad_arguments(self):
        range_from = -2
        range_to = -3
        self.builder()
        self.assertRaises(FailedExecution,
            self.ilo.filter_period, unit=self.unit, range_from=range_from,
            range_to=range_to, source='name', timestring=self.timestring, epoch=self.epoch
        )
    def test_missing_creation_date_raises(self):
        range_from = -1
        range_to = 0
        expected = []
        self.builder()
        self.ilo.get_index_state()
        self.ilo.get_index_settings()
        self.ilo.index_info['index-2016.03.03']['age'].pop('creation_date')
        self.ilo.index_info['index-2016.03.04']['age'].pop('creation_date')
        self.ilo.filter_period(unit=self.unit, range_from=range_from, range_to=range_to,
            source='creation_date', epoch=self.epoch)
        self.assertEqual(expected, self.ilo.indices)
    def test_non_integer_range_value(self):
        self.builder()
        self.assertRaises(ConfigurationError, self.ilo.filter_period, range_from='invalid')

class TestPeriodFilterAbsolute(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_bad_period_type(self):
        self.builder()
        self.assertRaises(ValueError, self.ilo.filter_period, period_type='invalid')
    def test_none_value_raises(self):
        self.builder()
        self.assertRaises(
            ConfigurationError, self.ilo.filter_period, period_type='absolute', date_from=None)
    def test_fail_on_bad_date(self):
        unit = 'months'
        date_from =  '2016.17'
        date_from_format = '%Y.%m'
        date_to = '2017.01'
        date_to_format = '%Y.%m'
        self.builder()
        self.assertRaises(
            FailedExecution,
            self.ilo.filter_period, unit=unit, source='creation_date', period_type='absolute', date_from=date_from,
            date_to=date_to, date_from_format=date_from_format, date_to_format=date_to_format
        )

class TestIndexListFilterBySize(TestCase):
    def builder(self, key='2'):
        self.client = Mock()
        self.client.info.return_value = get_es_ver()
        self.client.cat.indices.return_value = get_testvals(key, 'state')
        self.client.indices.get_settings.return_value = get_testvals(key, 'settings')
        self.client.indices.stats.return_value = get_testvals(key, 'stats')
        self.client.field_stats.return_value = get_testvals(key, 'fieldstats')
        self.client.indices.exists_alias.return_value = False
        self.ilo = IndexList(self.client)
    def test_missing_size_value(self):
        self.builder()
        self.assertRaises(MissingArgument, self.ilo.filter_by_size)
    def test_filter_default_result(self):
        self.builder()
        self.ilo.filter_by_size(size_threshold=0.52)
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_filter_default_result_and_exclude(self):
        self.builder()
        self.ilo.filter_by_size(size_threshold=0.52, exclude=True)
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
    def test_filter_by_threshold_behavior_less_than(self):
        self.builder()
        self.ilo.filter_by_size(size_threshold=0.52, threshold_behavior='less_than')
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
    def test_filter_by_size_behavior_total(self):
        self.builder()
        self.ilo.filter_by_size(size_threshold=1.04, size_behavior='total')
        self.assertEqual(['index-2016.03.04'], self.ilo.indices)
    def test_filter_by_size_behavior_total_and_threshold_behavior_less_than(self):
        self.builder()
        self.ilo.filter_by_size(size_threshold=1.04, size_behavior='total', threshold_behavior='less_than')
        self.assertEqual(['index-2016.03.03'], self.ilo.indices)
