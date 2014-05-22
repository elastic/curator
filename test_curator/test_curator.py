from datetime import datetime, timedelta
from unittest import TestCase
from mock import Mock

import curator


class TestUtils(TestCase):
    def test_get_index_time(self):
        for text, sep, dt in [
            ('2014.01.19', '.', datetime(2014, 1, 19)),
            ('2014-01-19', '-', datetime(2014, 1, 19)),
            ('2010-12-29', '-', datetime(2010, 12, 29)),
            ('2010.12.29.12', '.', datetime(2010, 12, 29, 12)),
                ]:
            self.assertEqual(dt, curator.get_index_time(text, sep))

class TestShowIndices(TestCase):
    def test_show_indices(self):
        client = Mock()
        client.indices.get_settings.return_value = {
            'prefix-2014.01.03': True,
            'prefix-2014.01.02': True,
            'prefix-2014.01.01': True
        }
        indices = curator.get_indices(client, prefix='prefix-')

        self.assertEquals([
                'prefix-2014.01.01',
                'prefix-2014.01.02',
                'prefix-2014.01.03', 
            ],
            indices
        )

class TestExpireIndices(TestCase):
    def test_all_daily_indices_found(self):
        client = Mock()
        client.indices.get_settings.return_value = {
            'prefix-2014.01.03': True,
            'prefix-2014.01.02': True,
            'prefix-2014.01.01': True,
            'prefix-2013.12.31': True,
            'prefix-2013.12.30': True,
            'prefix-2013.12.29': True,

            'prefix-2013.01.03': True,
            'prefix-2013.01.03.10': True,
        }
        expired = curator.find_expired_indices(client, 'days', 4, prefix='prefix-', utc_now=datetime(2014, 1, 3))
        
        expired = list(expired)

        self.assertEquals([
                ('prefix-2013.01.03', timedelta(days=362)),
                ('prefix-2013.12.29', timedelta(days=2)),
                ('prefix-2013.12.30', timedelta(days=1)),
            ],
            expired
        )

    def test_size_based_finds_indices_over_threshold(self):
        client = Mock()
        client.indices.status.return_value = {
            'indices': {
                'logstash-2014.02.14': {'index': {'primary_size_in_bytes': 3 * 2**30}},
                'logstash-2014.02.13': {'index': {'primary_size_in_bytes': 2 * 2**30}},
                'logstash-2014.02.12': {'index': {'primary_size_in_bytes': 1 * 2**30}},
                'logstash-2014.02.11': {'index': {'primary_size_in_bytes': 3 * 2**30}},
                'logstash-2014.02.10': {'index': {'primary_size_in_bytes': 3 * 2**30}},
            }        
        }
        expired = curator.find_overusage_indices(client, 6)
        expired = list(expired)

        self.assertEquals(
            [
                ('logstash-2014.02.11', 0),
                ('logstash-2014.02.10', 0),
            ],
            expired
        )

class TestPregenerateIndices(TestCase):
    def test_daily(self):
        result = set(curator.get_indices_to_create('days', utc_now=datetime(2014, 1, 3, 6, 5)))
        self.assertEquals(2, len(result))
        self.assertTrue('logstash-2014.01.03' in result)
        self.assertTrue('logstash-2014.01.04' in result)
        result = set(curator.get_indices_to_create('days', prefix='prefix-', separator='-', days_in_advance=3, utc_now=datetime(2014, 1, 3)))
        self.assertEquals(4, len(result))
        self.assertTrue('prefix-2014-01-03' in result)
        self.assertTrue('prefix-2014-01-04' in result)
        self.assertTrue('prefix-2014-01-05' in result)
        self.assertTrue('prefix-2014-01-06' in result)

    def test_hourly(self):
        result = set(curator.get_indices_to_create('hours', utc_now=datetime(2014, 1, 3, 6, 12)))
        self.assertEquals(48, len(result))
        self.assertTrue('logstash-2014.01.03.00' in result)
        self.assertTrue('logstash-2014.01.03.23' in result)
        self.assertTrue('logstash-2014.01.04.00' in result)
        self.assertTrue('logstash-2014.01.04.23' in result)
        result = set(curator.get_indices_to_create('hours', prefix='prefix-', separator='-', days_in_advance=3, utc_now=datetime(2014, 1, 3)))
        self.assertEquals(96, len(result))
        self.assertTrue('prefix-2014-01-03-00' in result)
        self.assertTrue('prefix-2014-01-03-23' in result)
        self.assertTrue('prefix-2014-01-04-00' in result)
        self.assertTrue('prefix-2014-01-04-23' in result)
        self.assertTrue('prefix-2014-01-05-00' in result)
        self.assertTrue('prefix-2014-01-05-23' in result)
        self.assertTrue('prefix-2014-01-06-00' in result)
        self.assertTrue('prefix-2014-01-06-23' in result)

