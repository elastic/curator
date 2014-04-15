from datetime import datetime, timedelta

import curator

from . import CuratorTestCase

class TestTimeBasedDeletion(CuratorTestCase):
    def test_curator_will_properly_delete_indices(self):
        self.create_indices(10)
        self.run_curator(delete_older=3)
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(3, len(mtd['metadata']['indices'].keys()))

    def test_curator_will_properly_delete_hourly_indices(self):
        self.create_indices(10, 'hours')
        self.run_curator(delete_older=3, time_unit='hours')
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(3, len(mtd['metadata']['indices'].keys()))

class TestFindExpiredIndices(CuratorTestCase):
    def test_find_closed_indices(self):
        self.create_index('l-2014.01.03')
        self.client.indices.close(index='l-2014.01.03')
        self.create_index('l-2014.01.01')

        # all indices should be expired
        expired = list(curator.find_expired_indices(self.client, 'days', 1,
            utc_now=datetime(2014, 1, 8, 3, 45, 50), prefix='l-'))

        self.assertEquals(
            [
                ('l-2014.01.01', timedelta(7)),
                ('l-2014.01.03', timedelta(5)),
            ],
            expired
        )

    def test_find_indices_ignores_indices_with_different_prefix_or_time_unit(self):
        self.create_index('logstash-2012.01.01')        # wrong precision
        self.create_index('not-logstash-2012.01.01.00') # wrong prefix
        self.create_index('logstash-2012.01.01.00')

        expired = list(curator.find_expired_indices(self.client, 'hours', 1))
        self.assertEquals(1, len(expired))
        self.assertEquals('logstash-2012.01.01.00', expired[0][0])

    def test_find_reports_correct_time_interval_from_cutoff(self):
        self.create_index('l-2014.01.01')
        self.create_index('l-2014.01.02')
        # yesterday is always save since we reset to mignight and do <, not <=
        self.create_index('l-2014.01.03')

        expired = list(curator.find_expired_indices(self.client, 'days', 1,
            utc_now=datetime(2014, 1, 4, 3, 45, 50), prefix='l-'))
        self.assertEquals(
            [
                (u'l-2014.01.01', timedelta(3)),
                (u'l-2014.01.02', timedelta(2)),
                (u'l-2014.01.03', timedelta(1)),
            ],
            expired
        )

