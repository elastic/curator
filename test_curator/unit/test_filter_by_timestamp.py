from datetime import datetime, timedelta
from unittest import TestCase

import curator


class TestFilterByTimestamp(TestCase):
    def test_filter_by_timestamp(self):
        object_list = ['logstash-2014.01.01', 'logstash-2014.01.02',
                       'logstash-2014.01.03', 'logstash-2014.01.04']
        timestring = '%Y.%m.%d'
        utc_now = datetime(2014, 1, 4, 0, 00, 00)
        res = curator.filter_by_timestamp(object_list=object_list, suffix='',
                                          timestring=timestring,
                                          time_unit='days', older_than=1,
                                          not_older_than=3, prefix='logstash-',
                                          utc_now=utc_now)
        assert list(res) == ['logstash-2014.01.03']
