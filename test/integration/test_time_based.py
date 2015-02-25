from datetime import datetime, timedelta

import curator

from . import CuratorTestCase

class TestTimeBasedDeletion(CuratorTestCase):
    def test_curator_will_properly_delete_indices(self):
        self.create_indices(10)
        curator.delete(self.client, older_than=3, timestring='%Y.%m.%d')
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(3, len(mtd['metadata']['indices'].keys()))

    def test_curator_will_properly_delete_hourly_indices(self):
        self.create_indices(10, 'hours')
        curator.delete(self.client, time_unit='hours', older_than=3, timestring='%Y.%m.%d.%H')
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(3, len(mtd['metadata']['indices'].keys()))

    def test_curator_will_properly_delete_weely_indices(self):
        self.create_indices(10, 'weeks')
        curator.delete(self.client, time_unit='weeks', older_than=3, timestring='%Y.%W')
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(3, len(mtd['metadata']['indices'].keys()))

    def test_curator_will_properly_delete_monthly_indices(self):
        self.create_indices(10, 'months')
        curator.delete(self.client, time_unit='months', older_than=3, timestring='%Y.%m')
        mtd = self.client.cluster.state(index=self.args['prefix'] + '*', metric='metadata')
        self.assertEquals(4, len(mtd['metadata']['indices'].keys()))

class TestFindExpiredIndices(CuratorTestCase):
    def test_find_closed_indices(self):
        self.create_index('l-2014.01.03')
        self.client.indices.close(index='l-2014.01.03')
        self.create_index('l-2014.01.01')

        # all indices should be expired
        index_list = curator.get_object_list(self.client, prefix='l-')
        expired = list(curator.filter_by_timestamp(time_unit='days', older_than=1, timestring='%Y.%m.%d', object_list=index_list,
            utc_now=datetime(2014, 1, 8, 3, 45, 50), prefix='l-'))

        self.assertEquals(
            [
                'l-2014.01.01',
                'l-2014.01.03',
            ],
            expired
        )

    def test_find_closed_weekly_indices(self):
        self.create_index('l-2014.03')
        self.client.indices.close(index='l-2014.03')
        self.create_index('l-2014.04')
    
        # all indices should be expired
        index_list = curator.get_object_list(self.client, prefix='l-')
        expired = list(curator.filter_by_timestamp(time_unit='weeks', older_than=1, timestring='%Y.%W', object_list=index_list,
            utc_now=datetime(2014, 2, 4, 0, 0, 0), prefix='l-'))
    
        self.assertEquals(
            [ 'l-2014.03',
              'l-2014.04',
            ],
            expired
        )

    def test_find_closed_monthly_indices(self):
        self.create_index('l-2014.03')
        self.client.indices.close(index='l-2014.03')
        self.create_index('l-2014.04')
    
        # all indices should be expired
        index_list = curator.get_object_list(self.client, prefix='l-')
        expired = list(curator.filter_by_timestamp(time_unit='months', older_than=1, timestring='%Y.%m', object_list=index_list,
            utc_now=datetime(2014, 6, 1, 0, 0, 0), prefix='l-'))
    
        self.assertEquals(
            [ 'l-2014.03',
              'l-2014.04',
            ],
            expired
        )

    def test_find_indices_ignores_indices_with_different_prefix_or_time_unit(self):
        self.create_index('logstash-2012.01.01')        # wrong precision
        self.create_index('logstash-2012.01')           # wrong precision
        self.create_index('not-logstash-2012.01.01.00') # wrong prefix
        self.create_index('logstash-2012.01.01.00')

        index_list = curator.get_object_list(self.client, prefix=self.args['prefix'])
        expired = list(curator.filter_by_timestamp(time_unit='hours', older_than=1, timestring='%Y.%m.%d.%H', object_list=index_list))
        self.assertEquals(1, len(expired))
        self.assertEquals('logstash-2012.01.01.00', expired[0])

    def test_find_reports_correct_time_interval_from_cutoff(self):
        self.create_index('l-2014.01.01')
        self.create_index('l-2014.01.02')
        # yesterday is always save since we reset to mignight and do <, not <=
        self.create_index('l-2014.01.03')

        index_list = curator.get_object_list(self.client, prefix='l-')
        expired = list(curator.filter_by_timestamp(time_unit='days', older_than=1, timestring='%Y.%m.%d', object_list=index_list,
            utc_now=datetime(2014, 1, 4, 3, 45, 50), prefix='l-'))
        self.assertEquals(
            [ 'l-2014.01.01',
              'l-2014.01.02',
              'l-2014.01.03',
            ],
            expired
        )

    def test_find_reports_correct_month_interval_from_cutoff(self):
        self.create_index('l-2014.01')
        self.create_index('l-2014.02')
        # the last one is always saved since we reset to the first of the month and do <, not <=
        self.create_index('l-2014.03')
    
        index_list = curator.get_object_list(self.client, prefix='l-')
        expired = list(curator.filter_by_timestamp(time_unit='months', older_than=1, timestring='%Y.%m', object_list=index_list,
            utc_now=datetime(2014, 5, 1), prefix='l-'))
        self.assertEquals(
            [ 'l-2014.01',
              'l-2014.02',
              'l-2014.03',
            ],
            expired
        )

class TestTimeBasedAliasing(CuratorTestCase):
    def test_curator_will_properly_alias_and_unalias_indices(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_indices(10)
        curator.alias(self.client, prefix=self.args['prefix'], alias=alias, timestring='%Y.%m.%d', alias_older_than=3, unalias_older_than=None)
        self.assertEquals(8, len(self.client.indices.get_alias(name=alias)))
        curator.alias(self.client, prefix=self.args['prefix'], alias=alias, timestring='%Y.%m.%d', alias_older_than=None, unalias_older_than=3)
        self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))

    def test_curator_will_properly_alias_and_unalias_weekly_indices(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_indices(10, 'weeks')
        curator.alias(self.client, time_unit='weeks', prefix=self.args['prefix'], alias=alias, timestring='%Y.%W', alias_older_than=3, unalias_older_than=None)
        self.assertEquals(8, len(self.client.indices.get_alias(name=alias)))
        curator.alias(self.client, time_unit='weeks', prefix=self.args['prefix'], alias=alias, timestring='%Y.%W', alias_older_than=None, unalias_older_than=3)
        self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))

    def test_curator_will_properly_alias_and_unalias_monthly_indices(self):
        alias = 'testalias'
        self.create_index('dummy')
        self.client.indices.put_alias(index='dummy', name=alias)
        self.create_indices(10, 'months')
        curator.alias(self.client, time_unit='months', prefix=self.args['prefix'], alias=alias, timestring='%Y.%m', alias_older_than=3, unalias_older_than=None)
        self.assertEquals(7, len(self.client.indices.get_alias(name=alias)))
        curator.alias(self.client, time_unit='months', prefix=self.args['prefix'], alias=alias, timestring='%Y.%m', alias_older_than=None, unalias_older_than=3)
        self.assertEquals(1, len(self.client.indices.get_alias(name=alias)))
