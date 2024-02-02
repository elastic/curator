"""test_helpers_date_ops"""
from unittest import TestCase
from datetime import datetime
import pytest
from unittest.mock import Mock
from elasticsearch8 import NotFoundError
from elastic_transport import ApiResponseMeta
from curator.exceptions import ConfigurationError
from curator.helpers.date_ops import (
    absolute_date_range, date_range, datetime_to_epoch, fix_epoch, get_date_regex, get_datemath,
    get_point_of_reference, isdatemath
)

class TestGetDateRegex(TestCase):
    """TestGetDateRegex

    Test helpers.date_ops.get_date_regex functionality.
    """
    def test_non_escaped(self):
        """test_non_escaped

        Should return a proper regex from a non-escaped Python date string
        """
        assert '\\d{4}\\-\\d{2}\\-\\d{2}t\\d{2}' == get_date_regex('%Y-%m-%dt%H')

class TestFixEpoch(TestCase):
    """TestFixEpoch

    Test helpers.date_ops.fix_epoch functionality.
    """
    def test_fix_epoch(self):
        """test_fix_epoch

        Should return straight epoch time in seconds, removing milliseconds or more decimals
        """
        for long_epoch, epoch in [
            (1459287636, 1459287636),
            (14592876369, 14592876),
            (145928763699, 145928763),
            (1459287636999, 1459287636),
            (1459287636000000, 1459287636),
            (145928763600000000, 1459287636),
            (145928763600000001, 1459287636),
            (1459287636123456789, 1459287636),
                ]:
            assert epoch == fix_epoch(long_epoch)
    def test_fix_epoch_raise(self):
        """test_fix_epoch_raise

        Should raise a ``ValueError`` exception when an improper value is passed
        """
        with pytest.raises(ValueError):
            fix_epoch(None)

class TestGetPointOfReference(TestCase):
    """TestGetPointOfReference

    Test helpers.date_ops.get_point_of_reference functionality.
    """
    def test_get_point_of_reference(self):
        """test_get_point_of_reference

        Should return a reference point n units * seconds prior to the present epoch time
        """
        epoch = 1459288037
        for unit, result in [
            ('seconds', epoch-1),
            ('minutes', epoch-60),
            ('hours', epoch-3600),
            ('days', epoch-86400),
            ('weeks', epoch-(86400*7)),
            ('months', epoch-(86400*30)),
            ('years', epoch-(86400*365)),
                ]:
            # self.assertEqual(result, get_point_of_reference(unit, 1, epoch))
            assert result == get_point_of_reference(unit, 1, epoch)
    def test_get_por_raise(self):
        """test_get_por_raise

        Should raise a ``ValueError`` exception when an improper value is passed
        """
        self.assertRaises(ValueError, get_point_of_reference, 'invalid', 1)
        with pytest.raises(ValueError):
            get_point_of_reference('invalid', 1)

class TestDateRange(TestCase):
    """TestDateRange

    Test helpers.date_ops.date_range functionality.
    """
    EPOCH = datetime_to_epoch(datetime(2017,  4,  3, 22, 50, 17))
    def test_bad_unit(self):
        """test_bad_unit

        Should raise a ``ConfigurationError`` exception when an improper unit value is passed
        """
        with pytest.raises(ConfigurationError, match=r'"unit" must be one of'):
            date_range('invalid', 1, 1)
    def test_bad_range(self):
        """test_bad_range

        Should raise a ``ConfigurationError`` exception when an improper range value is passed
        """
        with pytest.raises(ConfigurationError, match=r'must be greater than or equal to'):
            date_range('hours', 1, -1)
    def test_hours_single(self):
        """test_hours_single

        Should match hard-coded values when range_from = -1 and range_to = -1 and unit is hours
        """
        unit = 'hours'
        range_from = -1
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  4,  3, 21,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  3, 21, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_hours_past_range(self):
        """test_hours_past_range

        Should match hard-coded values when range_from = -3 and range_to = -1 and unit is hours
        """
        unit = 'hours'
        range_from = -3
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  4,  3, 19,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  3, 21, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_hours_future_range(self):
        """test_hours_future_range

        Should match hard-coded values when range_from = 0 and range_to = 2 and unit is hours
        """
        unit = 'hours'
        range_from = 0
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  4,  3, 22,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  4, 00, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_hours_span_range(self):
        """test_hours_span_range

        Should match hard-coded values when range_from = -1 and range_to = 2 and unit is hours
        """
        unit = 'hours'
        range_from = -1
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  4,  3, 21,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  4, 00, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_days_single(self):
        """test_days_single

        Should match hard-coded values when range_from = -1 and range_to = -1 and unit is days
        """
        unit = 'days'
        range_from = -1
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  4,  2,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_days_past_range(self):
        """test_days_range

        Should match hard-coded values when range_from = -3 and range_to = -1 and unit is days
        """
        unit = 'days'
        range_from = -3
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  3, 31,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_days_future_range(self):
        """test_days_future_range

        Should match hard-coded values when range_from = 0 and range_to = 2 and unit is days
        """
        unit = 'days'
        range_from = 0
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  4,  3,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  5, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_days_span_range(self):
        """test_days_span_range

        Should match hard-coded values when range_from = -1 and range_to = 2 and unit is days
        """
        unit = 'days'
        range_from = -1
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  4,  2,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  5, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_weeks_single(self):
        """test_weeks_single

        Should match hard-coded values when range_from = -1 and range_to = -1 and unit is weeks
        """
        unit = 'weeks'
        range_from = -1
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  3, 26,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  1, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_weeks_past_range(self):
        """test_weeks_past_range

        Should match hard-coded values when range_from = -3 and range_to = -1 and unit is weeks
        """
        unit = 'weeks'
        range_from = -3
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  3, 12,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  1, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_weeks_future_range(self):
        """test_weeks_future_range

        Should match hard-coded values when range_from = 0 and range_to = 2 and unit is weeks
        """
        unit = 'weeks'
        range_from = 0
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  4,  2, 00,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4, 22, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_weeks_span_range(self):
        """test_weeks_span_range

        Should match hard-coded values when range_from = -1 and range_to = 2 and unit is weeks
        """
        unit = 'weeks'
        range_from = -1
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  3, 26,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4, 22, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_weeks_single_iso(self):
        """test_weeks_single_iso

        Should match hard-coded values when range_from = -1 and range_to = -1, unit is weeks,
        and ``week_starts_on`` = ``monday``
        """
        unit = 'weeks'
        range_from = -1
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  3, 27,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        # pylint: disable=line-too-long
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH, week_starts_on='monday')
    def test_weeks_past_range_iso(self):
        """test_weeks_past_range_iso

        Should match hard-coded values when range_from = -3 and range_to = -1, unit is weeks,
        and ``week_starts_on`` = ``monday``
        """
        unit = 'weeks'
        range_from = -3
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  3, 13,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4,  2, 23, 59, 59))
        # pylint: disable=line-too-long
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH, week_starts_on='monday')
    def test_weeks_future_range_iso(self):
        """test_weeks_future_range_iso

        Should match hard-coded values when range_from = 0 and range_to = 2, unit is weeks,
        and ``week_starts_on`` = ``monday``
        """
        unit = 'weeks'
        range_from = 0
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  4,  3,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4, 23, 23, 59, 59))
        # pylint: disable=line-too-long
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH, week_starts_on='monday')
    def test_weeks_span_range_iso(self):
        """test_weeks_span_range_iso

        Should match hard-coded values when range_from = -1 and range_to = 2, unit is weeks,
        and ``week_starts_on`` = ``monday``
        """
        unit = 'weeks'
        range_from = -1
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  3, 27,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  4, 23, 23, 59, 59))
        # pylint: disable=line-too-long
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH, week_starts_on='monday')
    def test_months_single(self):
        """test_months_single

        Should match hard-coded values when range_from = -1 and range_to = -1 and unit is months
        """
        unit = 'months'
        range_from = -1
        range_to = -1
        start = datetime_to_epoch(datetime(2017,  3,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  3, 31, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_months_past_range(self):
        """test_months_past_range

        Should match hard-coded values when range_from = -4 and range_to = -1 and unit is months
        """
        unit = 'months'
        range_from = -4
        range_to = -1
        start = datetime_to_epoch(datetime(2016, 12,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  3, 31, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_months_future_range(self):
        """test_months_future_range

        Should match hard-coded values when range_from = 7 and range_to = 10 and unit is months
        """
        unit = 'months'
        range_from = 7
        range_to = 10
        start = datetime_to_epoch(datetime(2017, 11,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2018,  2, 28, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_months_super_future_range(self):
        """test_months_super_future_range

        Should match hard-coded values when range_from = 9 and range_to = 10 and unit is months
        """
        unit = 'months'
        range_from = 9
        range_to = 10
        start = datetime_to_epoch(datetime(2018,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2018,  2, 28, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_months_span_range(self):
        """test_months_span_range

        Should match hard-coded values when range_from = -1 and range_to = 2 and unit is months
        """
        unit = 'months'
        range_from = -1
        range_to = 2

        start = datetime_to_epoch(datetime(2017,  3,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  6, 30, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_years_single(self):
        """test_years_single

        Should match hard-coded values when range_from = -1 and range_to = -1 and unit is years
        """
        unit = 'years'
        range_from = -1
        range_to = -1
        start = datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_years_past_range(self):
        """test_years_past_range

        Should match hard-coded values when range_from = -3 and range_to = -1 and unit is years
        """
        unit = 'years'
        range_from = -3
        range_to = -1
        start = datetime_to_epoch(datetime(2014,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_years_future_range(self):
        """test_years_future_range

        Should match hard-coded values when range_from = 0 and range_to = 2 and unit is years
        """
        unit = 'years'
        range_from = 0
        range_to = 2
        start = datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2019, 12, 31, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)
    def test_years_span_range(self):
        """test_years_span_range

        Should match hard-coded values when range_from = -1 and range_to = 2 and unit is years
        """
        unit = 'years'
        range_from = -1
        range_to = 2
        start = datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2019, 12, 31, 23, 59, 59))
        assert (start, end) == date_range(unit, range_from, range_to, epoch=self.EPOCH)

class TestAbsoluteDateRange(TestCase):
    """TestAbsoluteDateRange

    Test helpers.date_ops.absolute_date_range functionality.
    """
    def test_bad_unit(self):
        """test_bad_unit

        Should raise a ``ConfigurationError`` exception when an invalid value for unit is passed
        """
        unit = 'invalid'
        date_from = '2017.01'
        date_from_format = '%Y.%m'
        date_to = '2017.01'
        date_to_format = '%Y.%m'
        with pytest.raises(ConfigurationError, match=r'"unit" must be one of'):
            absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
    def test_bad_formats(self):
        """test_bad_formats

        Should raise a ``ConfigurationError`` exception when no value for ``date_from_format`` or
        ``date_to_format`` is passed.
        """
        unit = 'days'
        with pytest.raises(ConfigurationError, match=r'Must provide "date_from_format" and "date_to_format"'):
            absolute_date_range(unit, 'meh', 'meh', None, 'meh')
        with pytest.raises(ConfigurationError, match=r'Must provide "date_from_format" and "date_to_format"'):
            absolute_date_range(unit, 'meh', 'meh', 'meh', None)
    def test_bad_dates(self):
        """test_bad_dates

        Should raise a ``ConfigurationError`` exception when date formats cannot be parsed for
        ``date_from_format`` and ``date_to_format``
        """
        unit = 'weeks'
        date_from_format = '%Y.%m'
        date_to_format = '%Y.%m'
        with pytest.raises(ConfigurationError, match=r'Unable to parse "date_from"'):
            absolute_date_range(unit, 'meh', '2017.01', date_from_format, date_to_format)
        with pytest.raises(ConfigurationError, match=r'Unable to parse "date_to"'):
            absolute_date_range(unit, '2017.01', 'meh', date_from_format, date_to_format)
    def test_single_month(self):
        """test_single_month

        Output should match hard-coded values
        """
        unit = 'months'
        date_from = '2017.01'
        date_from_format = '%Y.%m'
        date_to = '2017.01'
        date_to_format = '%Y.%m'
        start = datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  1, 31, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_multiple_month(self):
        """test_multiple_month

        Output should match hard-coded values
        """
        unit = 'months'
        date_from = '2016.11'
        date_from_format = '%Y.%m'
        date_to = '2016.12'
        date_to_format = '%Y.%m'
        start = datetime_to_epoch(datetime(2016, 11,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2016, 12, 31, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_single_year(self):
        """test_single_year

        Output should match hard-coded values
        """
        unit = 'years'
        date_from = '2017'
        date_from_format = '%Y'
        date_to = '2017'
        date_to_format = '%Y'
        start = datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017, 12, 31, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_multiple_year(self):
        """test_multiple_year

        Output should match hard-coded values
        """
        unit = 'years'
        date_from = '2016'
        date_from_format = '%Y'
        date_to = '2017'
        date_to_format = '%Y'
        start = datetime_to_epoch(datetime(2016,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017, 12, 31, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_single_week_uw(self):
        """test_single_week_UW

        Output should match hard-coded values
        """
        unit = 'weeks'
        date_from = '2017-01'
        date_from_format = '%Y-%U'
        date_to = '2017-01'
        date_to_format = '%Y-%U'
        start = datetime_to_epoch(datetime(2017,  1,  2,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  1,  8, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_multiple_weeks_uw(self):
        """test_multiple_weeks_UW

        Output should match hard-coded values
        """
        unit = 'weeks'
        date_from = '2017-01'
        date_from_format = '%Y-%U'
        date_to = '2017-04'
        date_to_format = '%Y-%U'
        start = datetime_to_epoch(datetime(2017,  1,   2,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  1,  29, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_single_week_iso(self):
        """test_single_week_ISO

        Output should match hard-coded values
        """
        unit = 'weeks'
        date_from = '2014-01'
        date_from_format = '%G-%V'
        date_to = '2014-01'
        date_to_format = '%G-%V'
        start = datetime_to_epoch(datetime(2013, 12, 30,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2014,  1,  5, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_multiple_weeks_iso(self):
        """test_multiple_weeks_ISO

        Output should match hard-coded values
        """
        unit = 'weeks'
        date_from = '2014-01'
        date_from_format = '%G-%V'
        date_to = '2014-04'
        date_to_format = '%G-%V'
        start = datetime_to_epoch(datetime(2013, 12, 30,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2014,  1, 26, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_single_day(self):
        """test_single_day

        Output should match hard-coded values
        """
        unit = 'days'
        date_from = '2017.01.01'
        date_from_format = '%Y.%m.%d'
        date_to = '2017.01.01'
        date_to_format = '%Y.%m.%d'
        start = datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  1,  1, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_multiple_days(self):
        """test_multiple_days

        Output should match hard-coded values
        """
        unit = 'days'
        date_from = '2016.12.31'
        date_from_format = '%Y.%m.%d'
        date_to = '2017.01.01'
        date_to_format = '%Y.%m.%d'
        start = datetime_to_epoch(datetime(2016, 12, 31,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  1,  1, 23, 59, 59))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result
    def test_iso8601(self):
        """test_ISO8601

        Output should match hard-coded values
        """
        unit = 'seconds'
        date_from = '2017-01-01T00:00:00'
        date_from_format = '%Y-%m-%dT%H:%M:%S'
        date_to = '2017-01-01T12:34:56'
        date_to_format = '%Y-%m-%dT%H:%M:%S'
        start = datetime_to_epoch(datetime(2017,  1,  1,  0,  0,  0))
        end   = datetime_to_epoch(datetime(2017,  1,  1, 12, 34, 56))
        result = absolute_date_range(unit, date_from, date_to, date_from_format, date_to_format)
        assert (start, end) == result

class TestIsDateMath(TestCase):
    """TestIsDateMath

    Test helpers.date_ops.isdatemath functionality.
    """
    def test_positive(self):
        """test_positive

        Result should match hard-coded sample
        """
        data = '<encapsulated>'
        assert isdatemath(data)
    def test_negative(self):
        """test_negative

        Result should not match hard-coded sample
        """
        data = 'not_encapsulated'
        assert not isdatemath(data)
    def test_raises(self):
        """test_raises

        Should raise ConfigurationError exception when malformed data is passed
        """
        data = '<badly_encapsulated'
        with pytest.raises(ConfigurationError, match=r'Incomplete datemath encapsulation'):
            isdatemath(data)

class TestGetDateMath(TestCase):
    """TestGetDateMath

    Test helpers.date_ops.get_datemath functionality.
    """
    def test_success(self):
        """test_success

        Result should match hard-coded sample
        """
        client = Mock()
        datemath = '{hasthemath}'
        psuedo_random = 'not_random_at_all'
        expected = 'curator_get_datemath_function_' + psuedo_random + '-hasthemath'
        # 5 positional args for meta: status, http_version, headers, duration, node
        meta = ApiResponseMeta(404, '1.1', {}, 0.01, None)
        body = {'error':{'index':expected}}
        msg = 'index_not_found_exception'
        # 3 positional args for NotFoundError: message, meta, body
        effect = NotFoundError(msg, meta, body)
        client.indices.get.side_effect = effect
        self.assertEqual('hasthemath', get_datemath(client, datemath, psuedo_random))
    def test_failure(self):
        """test_failure

        Should raise ConfigurationError exception when index is not found
        """
        client = Mock()
        datemath = '{hasthemath}'
        # 5 positional args for meta: status, http_version, headers, duration, node
        meta = ApiResponseMeta(404, '1.1', {}, 0.01, None)
        body = {'error':{'index':'this_will_not_be_found'}}
        msg = 'index_not_found_exception'
        # 3 positional args for NotFoundError: message, meta, body
        effect = NotFoundError(msg, meta, body)
        client.indices.get.side_effect = effect
        self.assertRaises(ConfigurationError, get_datemath, client, datemath)
