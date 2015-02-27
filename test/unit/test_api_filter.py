from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock

from curator import api as curator

named_index    = 'index_name'
named_indices  = [ "index1", "index2" ]
open_index     = {'metadata': {'indices' : { named_index : {'state' : 'open'}}}}
closed_index   = {'metadata': {'indices' : { named_index : {'state' : 'close'}}}}
open_indices   = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'open' },
                                               'index2' : { 'state' : 'open' }}}}
closed_indices = { 'metadata': { 'indices' : { 'index1' : { 'state' : 'close' },
                                               'index2' : { 'state' : 'close' }}}}
fake_fail      = Exception('Simulated Failure')
indices_space  = { 'indices' : {
        'index1' : { 'index' : { 'primary_size_in_bytes': 1083741824 }},
        'index2' : { 'index' : { 'primary_size_in_bytes': 1083741824 }}}}
re_test_indices = [
    "logstash-2014.12.31", "logstash-2014.12.30", "logstash-2014.12.29",
    ".marvel-2015.12.31", ".marvel-2015.12.30", ".marvel-2015.12.29",
    "2014.11.30", "2014.11.29", "2014.12.28",
    "2014.10.31-buh", "2014.10.30-buh", "2014.10.29-buh",
    "2015-01-01", "2015-01-02", "2015-01-03",
    "20150101", "20150102", "20150103",
    "foo", "bar", "baz", "neeble",
    "logstash-foo", "logstash-bar",
    "foo-logstash", "bar-logstash",
    "foologstash", "barlogstash",
    "logstashfoo", "logstashbar",
    ]

class FilterBySpace(TestCase):
    def test_filter_by_space_param_check(self):
        client = Mock()
        # Testing for the omission of the disk_space param
        self.assertFalse(curator.filter_by_space(client, named_indices))
    def test_filter_by_space_all_indices_closed(self):
        client = Mock()
        ds = 100.0
        client.cluster.state.return_value = closed_indices
        self.assertEqual([], curator.filter_by_space(client, named_indices, disk_space=ds))
    def test_filter_by_space_no_deletions_positive(self):
        client = Mock()
        ds = 10.0
        client.cluster.state.return_value = open_indices
        # Build return value of over 1G in size for each index
        client.indices.status.return_value = indices_space
        self.assertEqual([], curator.filter_by_space(client, named_indices, disk_space=ds))
    def test_filter_by_space_one_deletion(self):
        client = Mock()
        ds = 2.0
        client.cluster.state.return_value = open_indices
        # Build return value of over 1G in size for each index
        client.indices.status.return_value = indices_space
        self.assertEqual(["index1"], curator.filter_by_space(client, named_indices, disk_space=ds))
    def test_filter_by_space_one_deletion_no_reverse(self):
        client = Mock()
        ds = 2.0
        client.cluster.state.return_value = open_indices
        # Build return value of over 1G in size for each index
        client.indices.status.return_value = indices_space
        self.assertEqual(["index2"], curator.filter_by_space(client, named_indices, disk_space=ds, reverse=False))

class TestRegexIterate(TestCase):
    # The regex_iterate method filters a list of indices based on regex patterns
    def test_regex_iterate_filter_none(self):
        pattern = r'.*'
        self.assertEqual(re_test_indices, curator.regex_iterate(re_test_indices, pattern))
    def test_regex_iterate_prefix(self):
        pattern = r'^foo.*$'
        expected = [ 'foo', 'foo-logstash', 'foologstash' ]
        self.assertEqual(expected, curator.regex_iterate(re_test_indices, pattern))
    def test_regex_iterate_suffix(self):
        pattern = r'^.*foo$'
        expected = [ 'foo', 'logstash-foo', 'logstashfoo' ]
        self.assertEqual(expected, curator.regex_iterate(re_test_indices, pattern))
    def test_regex_iterate_timestring(self):
        pattern = r'^.*\d{4}.\d{2}.\d{2}.*$'
        expected = [
            'logstash-2014.12.31', 'logstash-2014.12.30', 'logstash-2014.12.29',
            '.marvel-2015.12.31', '.marvel-2015.12.30', '.marvel-2015.12.29',
            '2014.11.30', '2014.11.29', '2014.12.28', '2014.10.31-buh',
            '2014.10.30-buh', '2014.10.29-buh', '2015-01-01', '2015-01-02',
            '2015-01-03',
            ]
        self.assertEqual(expected, curator.regex_iterate(re_test_indices, pattern))
    def test_regex_iterate_newer_than(self):
        pattern = r'(?P<date>\d{4}.\d{2}.\d{2})'
        expected = [
            'logstash-2014.12.31', 'logstash-2014.12.30', 'logstash-2014.12.29',
            '.marvel-2015.12.31', '.marvel-2015.12.30', '.marvel-2015.12.29',
            '2014.12.28',
            ]
        t = datetime(2014, 12, 01, 2, 34, 56)
        self.assertEqual(expected,
            curator.regex_iterate(re_test_indices, pattern, groupname='date',
                timestring='%Y.%m.%d', time_unit='days', method='newer_than',
                value=2, utc_now=t))
    def test_regex_iterate_newer_than_negative_value(self):
        pattern = r'(?P<date>\d{4}.\d{2}.\d{2})'
        expected = [
            '.marvel-2015.12.31', '.marvel-2015.12.30', '.marvel-2015.12.29'
            ]
        t = datetime(2014, 12, 01, 2, 34, 56)
        self.assertEqual(expected,
            curator.regex_iterate(re_test_indices, pattern, groupname='date',
                timestring='%Y.%m.%d', time_unit='days', method='newer_than',
                value=-30, utc_now=t))
    def test_regex_iterate_older_than_date_only(self):
        pattern = r'(?P<date>\d{4}-\d{2}-\d{2})'
        expected = [
            '2015-01-01', '2015-01-02', '2015-01-03',
            ]
        t = datetime(2015, 2, 1, 2, 34, 56)
        self.assertEqual(expected,
            curator.regex_iterate(re_test_indices, pattern, groupname='date',
                timestring='%Y-%m-%d', time_unit='days', method='older_than',
                value=2, utc_now=t))
    def test_regex_iterate_older_than_with_prefix_and_suffix(self):
        pattern = r'(?P<date>\d{4}.\d{2}.\d{2})'
        expected = [
            'logstash-2014.12.31', 'logstash-2014.12.30', 'logstash-2014.12.29',
            '2014.11.30', '2014.11.29', '2014.12.28', '2014.10.31-buh',
            '2014.10.30-buh', '2014.10.29-buh'
            ]
        t = datetime(2015, 2, 1, 2, 34, 56)
        self.assertEqual(expected,
            curator.regex_iterate(re_test_indices, pattern, groupname='date',
                timestring='%Y.%m.%d', time_unit='days', method='older_than',
                value=2, utc_now=t))

class TestGetDateRegex(TestCase):
    def test_get_date_regex_arbitrary(self):
        self.assertEqual('\\a\\a\\a\\a', curator.get_date_regex('aaaa'))
    def test_get_date_regex_arbitrary_with_percent(self):
        self.assertEqual('\\a\\a\\a\\a', curator.get_date_regex('%a%a%a%a'))
    def test_get_date_regex_date_map(self):
        self.assertEqual('\\d{4}\\.\\d{2}\\.\\d{2}\\-\\d{2}\\-\\d{2}\\d{2}\\d{2}',
            curator.get_date_regex('%Y.%y.%m-%W-%U%d%H'))


class TestGetIndexTime(TestCase):
    def test_get_index_time_week_fix_W(self):
        utc_now  = datetime(2015, 2, 01, 2, 34, 56)
        expected = datetime(2015, 1, 26, 0, 00, 00)
        weeknow  = utc_now.strftime('%Y-%W')
        self.assertEqual(expected, curator.get_index_time(weeknow, '%Y-%W'))
    def test_get_index_time_week_fix_U(self):
        utc_now  = datetime(2015, 2, 21, 2, 34, 56)
        expected = datetime(2015, 2, 16, 0, 00, 00)
        weeknow  = utc_now.strftime('%Y-%U')
        self.assertEqual(expected, curator.get_index_time(weeknow, '%Y-%U'))
    def test_get_index_time_month_fix_positive(self):
        utc_now  = datetime(2015, 2, 22, 2, 34, 56)
        expected = datetime(2015, 2, 01, 0, 00, 00)
        weeknow  = utc_now.strftime('%Y-%m')
        self.assertEqual(expected, curator.get_index_time(weeknow, '%Y-%m'))
    def test_get_index_time_month_fix_negative(self):
        utc_now  = datetime(2015, 2, 22, 2, 34, 56)
        expected = datetime(2015, 2, 22, 0, 00, 00)
        weeknow  = utc_now.strftime('%Y-%m-%d')
        self.assertEqual(expected, curator.get_index_time(weeknow, '%Y-%m-%d'))

class TestGetTargetMonth(TestCase):
    def test_get_target_month_same_year(self):
        before = datetime(2015, 2, 01, 2, 34, 56)
        after  = datetime(2015, 1, 01, 0, 0, 0)
        self.assertEqual(after, curator.get_target_month(1, utc_now=before))
    def test_get_target_negative_value_month_same_year(self):
        before = datetime(2015, 2, 01, 2, 34, 56)
        after  = datetime(2015, 3, 01, 0, 0, 0)
        self.assertEqual(after, curator.get_target_month(-1, utc_now=before))
    def test_get_target_month_previous_year(self):
        before = datetime(2015, 2, 01, 2, 34, 56)
        after  = datetime(2014, 12, 01, 0, 0, 0)
        self.assertEqual(after, curator.get_target_month(2, utc_now=before))
    def test_get_target_negative_value_month_next_year(self):
        before = datetime(2015, 2, 01, 2, 34, 56)
        after  = datetime(2016, 1, 01, 0, 0, 0)
        self.assertEqual(after, curator.get_target_month(-11, utc_now=before))

class TestGetCutoff(TestCase):
    def test_get_cutoff_param_check(self):
        # Testing for the omission of the unit_count param
        self.assertFalse(curator.get_cutoff())
    def test_get_cutoff_weeks(self):
        fakenow = datetime(2015, 2, 03, 4, 5, 6)
        cutoff  = datetime(2015, 2, 02, 0, 0, 0)
        self.assertEqual(cutoff, curator.get_cutoff(1, time_unit='weeks', utc_now=fakenow))
    def test_get_cutoff_months(self):
        fakenow = datetime(2015, 2, 03, 4, 5, 6)
        cutoff  = datetime(2015, 1, 01, 0, 0, 0)
        self.assertEqual(cutoff, curator.get_cutoff(1, time_unit='months', utc_now=fakenow))
    def test_get_cutoff_negtive_value_weeks(self):
        fakenow = datetime(2015, 2, 03, 4, 5, 6)
        cutoff  = datetime(2015, 3, 9, 0, 0, 0)
        self.assertEqual(cutoff, curator.get_cutoff(-5, time_unit='weeks', utc_now=fakenow))
    def test_get_cutoff_negtive_value_months(self):
        fakenow = datetime(2015, 2, 03, 4, 5, 6)
        cutoff  = datetime(2015, 7, 01, 0, 0, 0)
        self.assertEqual(cutoff, curator.get_cutoff(-5, time_unit='months', utc_now=fakenow))
