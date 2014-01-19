from datetime import datetime, timedelta
from unittest import TestCase, main
from mock import Mock

import curator


class TestUtils(TestCase):
    def test_can_bloom(self):
        client = Mock()

        client.info.return_value = {"version": {"number": "1.0.0"}}
        self.assertTrue(curator.can_bloom(client))

        client.info.return_value = {"version": {"number": "0.90.10"}}
        self.assertTrue(curator.can_bloom(client))

        client.info.return_value = {"version": {"number": "0.90.8"}}
        self.assertFalse(curator.can_bloom(client))

    def test_get_index_time(self):
        for text, sep, dt in [
            ('2014.01.19', '.', datetime(2014, 1, 19)),
            ('2014-01-19', '-', datetime(2014, 1, 19)),
            ('2010-12-29', '-', datetime(2010, 12, 29)),
            ('2010.12.29.12', '.', datetime(2010, 12, 29, 12)),
                ]:
            self.assertEqual(dt, curator.get_index_time(text, sep))

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
        expired = curator.find_expired_indices(client, 'days', 3, prefix='prefix-', utc_now=datetime(2014, 1, 3))
        
        expired = list(expired)

        self.assertEquals([
                ('prefix-2013.01.03', timedelta(days=362)),
                ('prefix-2013.12.29', timedelta(days=2)),
                ('prefix-2013.12.30', timedelta(days=1)),
            ],
            expired
        )


if __name__ == '__main__':
    main()
