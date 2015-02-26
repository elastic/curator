from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock

from curator import api as curator


class TestEnsureList(TestCase):
    def test_ensure_list_returns_lists(self):
        l = ["a", "b", "c", "d"]
        e = ["a", "b", "c", "d"]
        self.assertEqual(e, curator.ensure_list(l))
        l = "abcd"
        e = ["abcd"]
        self.assertEqual(e, curator.ensure_list(l))
        l = [["abcd","defg"], 1, 2, 3]
        e = [["abcd","defg"], 1, 2, 3]
        self.assertEqual(e, curator.ensure_list(l))
        l = {"a":"b", "c":"d"}
        e = [{"a":"b", "c":"d"}]
        self.assertEqual(e, curator.ensure_list(l))

class TestTo_CSV(TestCase):
    def test_to_csv_will_return_csv(self):
        l = ["a", "b", "c", "d"]
        c = "a,b,c,d"
        self.assertEqual(c, curator.to_csv(l))
    def test_to_csv_will_return_single(self):
        l = ["a"]
        c = "a"
        self.assertEqual(c, curator.to_csv(l))
    def test_to_csv_will_return_None(self):
        l = []
        self.assertIsNone(curator.to_csv(l))

class TestCheckCSV(TestCase):
    def test_check_csv_positive(self):
        c = "1,2,3"
        self.assertTrue(curator.check_csv(c))
    def test_check_csv_negative(self):
        c = "12345"
        self.assertFalse(curator.check_csv(c))
    def test_check_csv_list(self):
        l = ["1", "2", "3"]
        self.assertTrue(curator.check_csv(l))
    def test_check_csv_wrong_value(self):
        v = 123
        self.assertIsNone(curator.check_csv(v))

class TestPruneKibana(TestCase):
    def test_prune_kibana_positive(self):
        l = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".kibana",
            ".marvel-kibana", "kibana-int", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        r = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        self.assertEqual(r, curator.prune_kibana(l))
    def test_prune_kibana_negative(self):
        l = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        r = [
            "logstash-2015.02.25", "logstash-2015.02.24", ".marvel-2015.02.25",
            ".marvel-2015.02.24",
            ]
        self.assertEqual(r, curator.prune_kibana(l))
    def test_prune_kibana_empty(self):
        l = [ ".kibana", ".marvel-kibana", "kibana-int", ]
        r = []
        self.assertEqual(r, curator.prune_kibana(l))

class TestGetVersion(TestCase):
    def test_positive(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9'} }
        version = curator.get_version(client)
        self.assertEqual(version, (9,9,9))
    def test_negative(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9'} }
        version = curator.get_version(client)
        self.assertNotEqual(version, (8,8,8))
    def test_dev_version_4_dots(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9.dev'} }
        version = curator.get_version(client)
        self.assertEqual(version, (9,9,9))
    def test_dev_version_with_dash(self):
        client = Mock()
        client.info.return_value = {'version': {'number': '9.9.9-dev'} }
        version = curator.get_version(client)
        self.assertEqual(version, (9,9,9))

class TestIsMasterNode(TestCase):
    def test_positive(self):
        client = Mock()
        client.nodes.info.return_value = {
            'nodes': { "foo" : "bar"}
        }
        client.cluster.state.return_value = {
            "master_node" : "foo"
        }
        self.assertTrue(curator.is_master_node(client))
    def test_negative(self):
        client = Mock()
        client.nodes.info.return_value = {
            'nodes': { "bad" : "mojo"}
        }
        client.cluster.state.return_value = {
            "master_node" : "foo"
        }
        self.assertFalse(curator.is_master_node(client))

class TestGetIndexTime(TestCase):
    def test_get_index_time(self):
        for text, datestring, dt in [
            ('2014.01.19', '%Y.%m.%d', datetime(2014, 1, 19)),
            ('14.01.19', '%y.%m.%d', datetime(2014, 1, 19)),
            ('2014-01-19', '%Y-%m-%d', datetime(2014, 1, 19)),
            ('2010-12-29', '%Y-%m-%d', datetime(2010, 12, 29)),
            ('2012-12', '%Y-%m', datetime(2012, 12, 1)),
            ('2011.01', '%Y.%m', datetime(2011, 1, 1)),
            ('2014-28', '%Y-%W', datetime(2014, 7, 14)),
            ('2010.12.29.12', '%Y.%m.%d.%H', datetime(2010, 12, 29, 12)),
                ]:
            self.assertEqual(dt, curator.get_index_time(text, datestring))
# class TestShowIndices(TestCase):
    # def test_show_indices(self):
    #     client = Mock()
    #     client.indices.get_settings.return_value = {
    #         'prefix-2014.01.03': True,
    #         'prefix-2014.01.02': True,
    #         'prefix-2014.01.01': True
    #     }
    #     indices = curator.get_indices(client, prefix='prefix-')
    #
    #     self.assertEquals([
    #             'prefix-2014.01.01',
    #             'prefix-2014.01.02',
    #             'prefix-2014.01.03',
    #         ],
    #         indices
    #     )
    #
    # def test_show_indices_with_suffix(self):
    #     client = Mock()
    #     client.indices.get_settings.return_value = {
    #         'prefix-2014.01.03-suffix': True,
    #         'prefix-2014.01.02-suffix': True,
    #         'prefix-2014.01.01-suffix': True
    #     }
    #     indices = curator.get_indices(client, prefix='prefix-', suffix='-suffix')
    #
    #     self.assertEquals([
    #             'prefix-2014.01.01-suffix',
    #             'prefix-2014.01.02-suffix',
    #             'prefix-2014.01.03-suffix',
    #         ],
    #         indices
    #     )
    #
    # def test_show_indices_with_suffix_and_no_prefix(self):
    #     client = Mock()
    #     client.indices.get_settings.return_value = {
    #         '2014.01.03-suffix': True,
    #         '2014.01.02-suffix': True,
    #         '2014.01.01-suffix': True
    #     }
    #     indices = curator.get_indices(client, prefix='', suffix='-suffix')
    #
    #     self.assertEquals([
    #             '2014.01.01-suffix',
    #             '2014.01.02-suffix',
    #             '2014.01.03-suffix',
    #         ],
    #         indices
    #     )

# class TestExpireIndices(TestCase):
#     def test_all_daily_indices_found(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             'prefix-2014.01.03': True,
#             'prefix-2014.01.02': True,
#             'prefix-2014.01.01': True,
#             'prefix-2013.12.31': True,
#             'prefix-2013.12.30': True,
#             'prefix-2013.12.29': True,
#
#             'prefix-2013.01.03': True,
#             'prefix-2013.01.03.10': True,
#             'prefix-2013.01': True,
#             'prefix-2013.12': True,
#             'prefix-2013.51': True,
#         }
#         index_list = curator.get_object_list(client, prefix='prefix-', suffix='')
#         expired = curator.filter_by_timestamp(object_list=index_list, time_unit='days', older_than=4, prefix='prefix-', suffix='', timestring='%Y.%m.%d', utc_now=datetime(2014, 1, 3))
#
#         expired = list(expired)
#
#         self.assertEquals([
#                 'prefix-2013.01.03',
#                 'prefix-2013.12.29',
#                 'prefix-2013.12.30',
#             ],
#             expired
#         )
#
#     def test_all_daily_indices_found_with_suffix(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             'prefix-2014.01.03-suffix': True,
#             'prefix-2014.01.02': True,
#             'prefix-2014.01.01-suffix': True,
#             'prefix-2013.12.31': True,
#             'prefix-2013.12.30-suffix': True,
#             'prefix-2013.12.29': True,
#
#             'prefix-2013.01.03-suffix': True,
#             'prefix-2013.01.03.10': True,
#             'prefix-2013.01': True,
#             'prefix-2013.12-suffix': True,
#             'prefix-2013.51': True,
#         }
#         index_list = curator.get_object_list(client, prefix='prefix-', suffix='-suffix')
#         expired = curator.filter_by_timestamp(object_list=index_list, time_unit='days', older_than=4, prefix='prefix-', suffix='-suffix', timestring='%Y.%m.%d', utc_now=datetime(2014, 1, 3))
#
#         expired = list(expired)
#
#         self.assertEquals([
#                 'prefix-2013.01.03-suffix',
#                 'prefix-2013.12.30-suffix',
#             ],
#             expired
#         )
#
#     def test_all_daily_indices_found_with_suffix_and_no_prefix(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             '2014.01.03-suffix': True,
#             'prefix-2014.01.02': True,
#             '2014.01.01-suffix': True,
#             'prefix-2013.12.31': True,
#             '2013.12.30-suffix': True,
#             'prefix-2013.12.29': True,
#
#             '2013.01.03-suffix': True,
#             '2013.01.03.10': True,
#             'prefix-2013.01': True,
#             'prefix-2013.12-suffix': True,
#             'prefix-2013.51': True,
#         }
#         index_list = curator.get_object_list(client, prefix='', suffix='-suffix')
#         expired = curator.filter_by_timestamp(object_list=index_list, time_unit='days', older_than=4, prefix='', suffix='-suffix', timestring='%Y.%m.%d', utc_now=datetime(2014, 1, 3))
#
#         expired = list(expired)
#
#         self.assertEquals([
#                 '2013.01.03-suffix',
#                 '2013.12.30-suffix',
#             ],
#             expired
#         )
#
#     def test_all_daily_indices_found_with_no_suffix_and_no_prefix(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             '2014.01.03': True,
#             '2014.01.02': True,
#             '2014.01.01': True,
#             '2013.12.31': True,
#             '2013.12.30': True,
#             '2013.12.29': True,
#
#             '2013.01.03': True,
#             '2013.01.03.10': True,
#             '2013.01': True,
#             '2013.12': True,
#             '2013.51': True,
#         }
#         index_list = curator.get_object_list(client, prefix='', suffix='')
#         expired = curator.filter_by_timestamp(object_list=index_list, time_unit='days', older_than=4, prefix='', suffix='', timestring='%Y.%m.%d', utc_now=datetime(2014, 1, 3))
#
#         expired = list(expired)
#
#         self.assertEquals([
#                 '2013.01.03',
#                 '2013.12.29',
#                 '2013.12.30',
#             ],
#             expired
#         )
#
#     def test_all_daily_indices_found_with_wildcard_prefix(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             'log-2014.01.03': True,
#             'log-2014.01.02': True,
#             'log-2014.01.01': True,
#             'log0-2013.12.31': True,
#             'logstash-2013.12.30': True,
#             'l-2013.12.29': True,
#
#             'prod-2013.01.03': True,
#             'cert-2013.01.03.10': True,
#             'test-2013.01': True,
#             'fail-2013.12': True,
#             'index-2013.51': True,
#         }
#         index_list = curator.get_object_list(client, prefix='l.*', suffix='')
#         expired = curator.filter_by_timestamp(object_list=index_list, time_unit='days', older_than=4, prefix='l.*', suffix='', timestring='%Y.%m.%d', utc_now=datetime(2014, 1, 3))
#
#         expired = list(expired)
#
#         self.assertEquals([
#                 'l-2013.12.29',
#                 'logstash-2013.12.30',
#             ],
#             expired
#         )
#
#     def test_all_daily_indices_found_with_wildcard_prefix_and_suffix(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             'log-2014.01.03-bar': True,
#             'log-2014.01.02-baz': True,
#             'log-2014.01.01-b': True,
#             'log0-2013.12.31-c': True,
#             'logstash-2013.12.30-bigdata': True,
#             'l-2013.12.29-closet': True,
#
#             'prod-2013.01.03-basketball': True,
#             'cert-2013.01.03.10-a': True,
#             'test-2013.01-d': True,
#             'fail-2013.12-f': True,
#             'index-2013.51-e': True,
#         }
#         index_list = curator.get_object_list(client, prefix='l.*', suffix='-b.*')
#         expired = curator.filter_by_timestamp(object_list=index_list, time_unit='days', older_than=4, prefix='l.*', suffix='-b.*', timestring='%Y.%m.%d', utc_now=datetime(2014, 1, 3))
#
#         expired = list(expired)
#
#         self.assertEquals([
#                 'logstash-2013.12.30-bigdata',
#             ],
#             expired
#         )
#
#     def test_all_daily_indices_found_with_star_prefix_and_suffix(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             'log-2014.01.03-bar': True,
#             'log-2014.01.02-baz': True,
#             'log-2014.01.01-b': True,
#             'log0-2013.12.31-c': True,
#             'logstash-2013.12.30-bigdata': True,
#             'l-2013.12.29-closet': True,
#
#             'prod-2013.01.03-basketball': True,
#             'cert-2013.01.03.10-a': True,
#             'test-2013.01-d': True,
#             'fail-2013.12-f': True,
#             'index-2013.51-e': True,
#         }
#         index_list = curator.get_object_list(client, prefix='*', suffix='*')
#         expired = curator.filter_by_timestamp(object_list=index_list, time_unit='days', older_than=4, prefix='*', suffix='*', timestring='%Y.%m.%d', utc_now=datetime(2014, 1, 3))
#
#         expired = list(expired)
#
#         self.assertEquals([
#                 'cert-2013.01.03.10-a',
#                 'l-2013.12.29-closet',
#                 'logstash-2013.12.30-bigdata',
#                 'prod-2013.01.03-basketball',
#             ],
#             expired
#         )
#
#     def test_size_based_finds_indices_over_threshold(self):
#         client = Mock()
#         client.indices.get_settings.return_value = {
#             'logstash-2014.02.14': True,
#             'logstash-2014.02.13': True,
#             'logstash-2014.02.12': True,
#             'logstash-2014.02.11': True,
#             'logstash-2014.02.10': True,
#         }
#         client.cluster.state.return_value = {
#             'metadata': {
#                 'indices': {
#                     'logstash-2014.02.14': {
#                         'state' : 'open'
#                     },
#                     'logstash-2014.02.13': {
#                          'state' : 'open'
#                     },
#                     'logstash-2014.02.12': {
#                         'state' : 'open'
#                     },
#                     'logstash-2014.02.11': {
#                         'state' : 'open'
#                     },
#                     'logstash-2014.02.10': {
#                         'state' : 'open'
#                     },
#                 }
#             }
#         }
#         client.indices.status.return_value = {
#             'indices': {
#                 'logstash-2014.02.14': {'index': {'primary_size_in_bytes': 3 * 2**30}},
#                 'logstash-2014.02.13': {'index': {'primary_size_in_bytes': 2 * 2**30}},
#                 'logstash-2014.02.12': {'index': {'primary_size_in_bytes': 1 * 2**30}},
#                 'logstash-2014.02.11': {'index': {'primary_size_in_bytes': 3 * 2**30}},
#                 'logstash-2014.02.10': {'index': {'primary_size_in_bytes': 3 * 2**30}},
#             }
#         }
#         expired = curator.filter_by_space(client, disk_space=6)
#         expired = list(expired)
#
#         self.assertEquals(
#             [
#                 'logstash-2014.02.11',
#                 'logstash-2014.02.10',
#             ],
#             expired
#         )
