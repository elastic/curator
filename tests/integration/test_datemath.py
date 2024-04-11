"""Test date math with indices"""
# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
from datetime import datetime, timedelta, timezone
from curator.helpers.date_ops import parse_datemath

from . import CuratorTestCase

class TestParseDateMath(CuratorTestCase):
    ### This test will remain commented until https://github.com/elastic/elasticsearch/issues/92892 is resolved
    # def test_function_positive(self):
    #     # test_string = '<.prefix-{2001-01-01-13|+1h/h{yyyy-MM-dd-HH|-07:00}}-suffix>'
    #     # expected = '.prefix-2001-01-01-14-suffix'
    #     assert expected == parse_datemath(self.client, test_string)
    def test_assorted_datemaths(self):
        for test_string, expected in [
            ('<prefix-{now}-suffix>', f"prefix-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}-suffix"),
            ('<prefix-{now-1d/d}>', f"prefix-{(datetime.now(timezone.utc)-timedelta(days=1)).strftime('%Y.%m.%d')}"),
            ('<{now+1d/d}>', f"{(datetime.now(timezone.utc)+timedelta(days=1)).strftime('%Y.%m.%d')}"),
            ('<{now+1d/d}>', f"{(datetime.now(timezone.utc)+timedelta(days=1)).strftime('%Y.%m.%d')}"),
            ('<{now+10d/d{yyyy-MM}}>', f"{(datetime.now(timezone.utc)+timedelta(days=10)).strftime('%Y-%m')}"),
            ### This test will remain commented until https://github.com/elastic/elasticsearch/issues/92892 is resolved
            # ('<{now+10d/h{yyyy-MM-dd-HH|-07:00}}>', f"{(datetime.now(timezone.utc)+timedelta(days=10)-timedelta(hours=7)).strftime('%Y-%m-%d-%H')}"),
          ]:
            assert expected == parse_datemath(self.client, test_string)
