"""Test date math with indices"""

# pylint: disable=C0115, C0116, invalid-name
from datetime import datetime, timedelta, timezone
from unittest import SkipTest
from curator.helpers.date_ops import parse_datemath

from . import CuratorTestCase


class TestParseDateMath(CuratorTestCase):
    colonblow = (
        'Date math expressions with colons are not supported in Elasticsearch '
        'versions 8.0 - 8.6'
    )

    def test_assorted_datemaths(self):
        now = datetime.now(timezone.utc)
        oneday = timedelta(days=1)
        ymd = '%Y.%m.%d'
        for test_string, expected in [
            (
                '<prefix-{now}-suffix>',
                f"prefix-{now.strftime(ymd)}-suffix",
            ),
            (
                '<prefix-{now-1d/d}>',
                f"prefix-{(now-oneday).strftime(ymd)}",
            ),
            (
                '<{now+1d/d}>',
                f"{(now+oneday).strftime(ymd)}",
            ),
            (
                '<{now+1d/d}>',
                f"{(now+oneday).strftime(ymd)}",
            ),
            (
                '<{now+10d/d{yyyy-MM}}>',
                f"{(now+timedelta(days=10)).strftime('%Y-%m')}",
            ),
        ]:
            assert expected == parse_datemath(self.client, test_string)

    def test_complex_datemath(self):
        v = self.get_version()
        if (8, 0, 0) < v < (8, 7, 0):
            raise SkipTest(f'{self.colonblow} (current version: {v})')
        now = datetime.now(timezone.utc)
        _ = now + timedelta(days=10) - timedelta(hours=7)
        test_string = '<{now+10d/h{yyyy-MM-dd-HH|-07:00}}>'
        expected = f"{_.strftime('%Y-%m-%d-%H')}"
        assert expected == parse_datemath(self.client, test_string)

    def test_very_complex_datemaths(self):
        v = self.get_version()
        if (8, 0, 0) < v < (8, 7, 0):
            raise SkipTest(f'{self.colonblow} (current version: {v})')
        test_string = '<.prefix-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}-suffix>'
        expected = '.prefix-2001-01-01-14-suffix'
        assert expected == parse_datemath(self.client, test_string)
