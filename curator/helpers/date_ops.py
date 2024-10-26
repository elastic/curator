"""Curator date and time functions"""

import logging
import random
import re
import string
import time
from datetime import timedelta, datetime, timezone
from elasticsearch8.exceptions import NotFoundError
from curator.exceptions import ConfigurationError
from curator.defaults.settings import date_regex


class TimestringSearch:
    """
    An object to allow repetitive search against a string, ``searchme``, without
    having to repeatedly recreate the regex.

    :param timestring: An ``strftime`` pattern
    :type timestring: :py:func:`~.time.strftime`
    """

    def __init__(self, timestring):
        # pylint: disable=consider-using-f-string
        regex = r'(?P<date>{0})'.format(get_date_regex(timestring))

        #: Object attribute. ``re.compile(regex)`` where
        #: ``regex = r'(?P<date>{0})'.format(get_date_regex(timestring))``. Uses
        #: :py:func:`get_date_regex`
        self.pattern = re.compile(regex)
        #: Object attribute preserving param ``timestring``
        self.timestring = timestring

    def get_epoch(self, searchme):
        """
        :param searchme: A string to be matched against :py:attr:`pattern` that matches
            :py:attr:`timestring`

        :returns: The epoch timestamp extracted from ``searchme`` by regex matching
            against :py:attr:`pattern`
        :rtype: int or None
        """
        match = self.pattern.search(searchme)
        if match:
            if match.group("date"):
                timestamp = match.group("date")
                return datetime_to_epoch(get_datetime(timestamp, self.timestring))
            return None
        return None


def absolute_date_range(
    unit, date_from, date_to, date_from_format=None, date_to_format=None
):
    """
    This function calculates a date range with an absolute time stamp for both the
    start time and the end time. These dates are converted to epoch time. The parameter
    ``unit`` is used when the same simplified date is used for both ``date_from`` and
    ``date_to`` to calculate the duration. For example, if ``unit`` is ``months``, and
    ``date_from`` and ``date_to`` are both ``2017.01``, then the entire month of
    January 2017 will be the absolute date range.

    :param unit: One of ``hours``, ``days``, ``weeks``, ``months``, or ``years``.
    :param date_from: The simplified date for the start of the range
    :param date_to: The simplified date for the end of the range.
    :param date_from_format: The :py:func:`~.time.strftime` string used to parse
        ``date_from``
    :param date_to_format: The :py:func:`~.time.strftime` string used to parse
        ``date_to``

    :type unit: str
    :type date_from: str
    :type date_to: str
    :type date_from_format: str
    :type date_to_format: str

    :returns: The epoch start time and end time of a date range
    :rtype: tuple
    """
    logger = logging.getLogger(__name__)
    acceptable_units = [
        'seconds',
        'minutes',
        'hours',
        'days',
        'weeks',
        'months',
        'years',
    ]
    if unit not in acceptable_units:
        raise ConfigurationError(f'"unit" must be one of: {acceptable_units}')
    if not date_from_format or not date_to_format:
        raise ConfigurationError('Must provide "date_from_format" and "date_to_format"')
    try:
        start_epoch = datetime_to_epoch(get_datetime(date_from, date_from_format))
        logger.debug('Start ISO8601 = %s', epoch2iso(start_epoch))
    except Exception as err:
        raise ConfigurationError(
            f'Unable to parse "date_from" {date_from} and "date_from_format" '
            f'{date_from_format}. Error: {err}'
        ) from err
    try:
        end_date = get_datetime(date_to, date_to_format)
    except Exception as err:
        raise ConfigurationError(
            f'Unable to parse "date_to" {date_to} and "date_to_format" '
            f'{date_to_format}. Error: {err}'
        ) from err
    # We have to iterate to one more month, and then subtract a second to get
    # the last day of the correct month
    if unit == 'months':
        month = end_date.month
        year = end_date.year
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        new_end_date = datetime(year, month, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(new_end_date) - 1
    # Similarly, with years, we need to get the last moment of the year
    elif unit == 'years':
        new_end_date = datetime(end_date.year + 1, 1, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(new_end_date) - 1
    # It's not months or years, which have inconsistent reckoning...
    else:
        # This lets us use an existing method to simply add 1 more unit's worth
        # of seconds to get hours, days, or weeks, as they don't change
        # We use -1 as point of reference normally subtracts from the epoch
        # and we need to add to it, so we'll make it subtract a negative value.
        # Then, as before, subtract 1 to get the end of the period
        end_epoch = (
            get_point_of_reference(unit, -1, epoch=datetime_to_epoch(end_date)) - 1
        )

    logger.debug('End ISO8601 = %s', epoch2iso(end_epoch))
    return (start_epoch, end_epoch)


def date_range(unit, range_from, range_to, epoch=None, week_starts_on='sunday'):
    """
    This function calculates a date range with a distinct epoch time stamp of both the
    start time and the end time in counts of ``unit`` relative to the time at
    execution, if ``epoch`` is not set.

    If ``unit`` is ``weeks``, you can also determine when a week begins using
    ``week_starts_on``, which can be either ``sunday`` or ``monday``.

    :param unit: One of ``hours``, ``days``, ``weeks``, ``months``, or ``years``.
    :param range_from: Count of ``unit`` in the past/future is the origin?
    :param range_to: Count of ``unit`` in the past/future is the end point?
    :param epoch: An epoch timestamp used to establish a point of reference for
        calculations.
    :param week_starts_on: Either ``sunday`` or ``monday``. Default is ``sunday``

    :type unit: str
    :type range_from: int
    :type range_to: int
    :type epoch: int
    :type week_starts_on: str

    :returns: The epoch start time and end time of a date range
    :rtype: tuple
    """
    logger = logging.getLogger(__name__)
    start_date = None
    start_delta = None
    acceptable_units = ['hours', 'days', 'weeks', 'months', 'years']
    if unit not in acceptable_units:
        raise ConfigurationError(f'"unit" must be one of: {acceptable_units}')
    if not range_to >= range_from:
        raise ConfigurationError(
            '"range_to" must be greater than or equal to "range_from"'
        )
    if not epoch:
        epoch = time.time()
    epoch = fix_epoch(epoch)
    raw_point_of_ref = datetime.fromtimestamp(epoch, timezone.utc)
    logger.debug('Raw point of Reference = %s', raw_point_of_ref)
    # Reverse the polarity, because -1 as last week makes sense when read by
    # humans, but datetime timedelta math makes -1 in the future.
    origin = range_from * -1
    # These if statements help get the start date or start_delta
    if unit == 'hours':
        point_of_ref = datetime(
            raw_point_of_ref.year,
            raw_point_of_ref.month,
            raw_point_of_ref.day,
            raw_point_of_ref.hour,
            0,
            0,
        )
        start_delta = timedelta(hours=origin)
    if unit == 'days':
        point_of_ref = datetime(
            raw_point_of_ref.year, raw_point_of_ref.month, raw_point_of_ref.day, 0, 0, 0
        )
        start_delta = timedelta(days=origin)
    if unit == 'weeks':
        point_of_ref = datetime(
            raw_point_of_ref.year, raw_point_of_ref.month, raw_point_of_ref.day, 0, 0, 0
        )
        sunday = False
        if week_starts_on.lower() == 'sunday':
            sunday = True
        weekday = point_of_ref.weekday()
        # Compensate for ISO week starting on Monday by default
        if sunday:
            weekday += 1
        logger.debug('Weekday = %s', weekday)
        start_delta = timedelta(days=weekday, weeks=origin)
    if unit == 'months':
        point_of_ref = datetime(
            raw_point_of_ref.year, raw_point_of_ref.month, 1, 0, 0, 0
        )
        year = raw_point_of_ref.year
        month = raw_point_of_ref.month
        if origin > 0:
            for _ in range(0, origin):
                if month == 1:
                    year -= 1
                    month = 12
                else:
                    month -= 1
        else:
            for _ in range(origin, 0):
                if month == 12:
                    year += 1
                    month = 1
                else:
                    month += 1
        start_date = datetime(year, month, 1, 0, 0, 0)
    if unit == 'years':
        point_of_ref = datetime(raw_point_of_ref.year, 1, 1, 0, 0, 0)
        start_date = datetime(raw_point_of_ref.year - origin, 1, 1, 0, 0, 0)
    if unit not in ['months', 'years']:
        start_date = point_of_ref - start_delta
    # By this point, we know our start date and can convert it to epoch time
    start_epoch = datetime_to_epoch(start_date)
    logger.debug('Start ISO8601 = %s', epoch2iso(start_epoch))
    # This is the number of units we need to consider.
    count = (range_to - range_from) + 1
    # We have to iterate to one more month, and then subtract a second to get
    # the last day of the correct month
    if unit == 'months':
        month = start_date.month
        year = start_date.year
        for _ in range(0, count):
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
        end_date = datetime(year, month, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(end_date) - 1
    # Similarly, with years, we need to get the last moment of the year
    elif unit == 'years':
        end_date = datetime((raw_point_of_ref.year - origin) + count, 1, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(end_date) - 1
    # It's not months or years, which have inconsistent reckoning...
    else:
        # This lets us use an existing method to simply add unit * count seconds
        # to get hours, days, or weeks, as they don't change
        end_epoch = get_point_of_reference(unit, count * -1, epoch=start_epoch) - 1
    logger.debug('End ISO8601 = %s', epoch2iso(end_epoch))
    return (start_epoch, end_epoch)


def datetime_to_epoch(mydate):
    """
    Converts datetime into epoch seconds

    :param mydate: A Python datetime
    :type mydate: :py:class:`~.datetime.datetime`

    :returns: An epoch timestamp based on ``mydate``
    :rtype: int
    """
    tdelta = mydate - datetime(1970, 1, 1)
    return tdelta.seconds + tdelta.days * 24 * 3600


def epoch2iso(epoch: int) -> str:
    """
    Return an ISO8601 value for epoch

    :param epoch: An epoch timestamp
    :type epoch: int

    :returns: An ISO8601 timestamp
    :rtype: str
    """
    # Because Python 3.12 now requires non-naive timezone declarations, we must change.
    #
    # ## Example:
    # ## epoch == 1491256800
    # ##
    # ## The old way:
    # ##datetime.utcfromtimestamp(epoch)
    # ##   datetime.datetime(2017, 4, 3, 22, 0).isoformat()
    # ##   Result: 2017-04-03T22:00:00
    # ##
    # ## The new way:
    # ##     datetime.fromtimestamp(epoch, timezone.utc)
    # ##     datetime.datetime(
    # ##         2017, 4, 3, 22, 0, tzinfo=datetime.timezone.utc).isoformat()
    # ##     Result: 2017-04-03T22:00:00+00:00
    # ##
    # ## End Example
    #
    # Note that the +00:00 is appended now where we affirmatively declare the UTC
    # timezone
    #
    # As a result, we will use this function to prune away the timezone if it is +00:00
    # and replace it with Z, which is shorter Zulu notation for UTC (which
    # Elasticsearch uses)
    #
    # We are MANUALLY, FORCEFULLY declaring timezone.utc, so it should ALWAYS be
    # +00:00, but could in theory sometime show up as a Z, so we test for that.

    parts = datetime.fromtimestamp(epoch, timezone.utc).isoformat().split('+')
    if len(parts) == 1:
        if parts[0][-1] == 'Z':
            return parts[0]  # Our ISO8601 already ends with a Z for Zulu/UTC time
        return f'{parts[0]}Z'  # It doesn't end with a Z so we put one there
    if parts[1] == '00:00':
        return f'{parts[0]}Z'  # It doesn't end with a Z so we put one there
    return f'{parts[0]}+{parts[1]}'  # Fallback publishes the +TZ, whatever that was


def fix_epoch(epoch):
    """
    Fix value of ``epoch`` to be the count since the epoch in seconds only, which
    should be 10 or fewer digits long.

    :param epoch: An epoch timestamp, in epoch + milliseconds, or microsecond, or even
        nanoseconds.
    :type epoch: int

    :returns: An epoch timestamp in seconds only, based on ``epoch``
    :rtype: int
    """
    try:
        # No decimals allowed
        epoch = int(epoch)
    except Exception as err:
        raise ValueError(
            f'Bad epoch value. Unable to convert {epoch} to int. {err}'
        ) from err

    # If we're still using this script past January, 2038, we have bigger
    # problems than my hacky math here...
    if len(str(epoch)) <= 10:
        # Epoch is fine, no changes
        pass
    elif len(str(epoch)) > 10 and len(str(epoch)) <= 13:
        epoch = int(epoch / 1000)
    else:
        orders_of_magnitude = len(str(epoch)) - 10
        powers_of_ten = 10**orders_of_magnitude
        epoch = int(epoch / powers_of_ten)
    return epoch


def get_date_regex(timestring):
    """
    :param timestring: An ``strftime`` pattern
    :type timestring: :py:func:`~.time.strftime`

    :returns: A regex string based on a provided :py:func:`~.time.strftime`
        ``timestring``.
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    prev, regex = ('', '')
    logger.debug('Provided timestring = "%s"', timestring)
    for idx, char in enumerate(timestring):
        logger.debug('Current character: %s Array position: %s', char, idx)
        if char == '%':
            pass
        elif char in date_regex() and prev == '%':
            regex += r'\d{' + date_regex()[char] + '}'
        elif char in ['.', '-']:
            regex += "\\" + char
        else:
            regex += char
        prev = char
    logger.debug('regex = %s', regex)
    return regex


def get_datemath(client, datemath, random_element=None):
    """
    :param client: A client connection object
    :param datemath: An elasticsearch datemath string
    :param random_element: This allows external randomization of the name and is only
        useful for tests so that you can guarantee the output because you provided the
        random portion.

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type datemath: :py:class:`~.datemath.datemath`
    :type random_element: str

    :returns: the parsed index name from ``datemath``
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    if random_element is None:
        random_prefix = 'curator_get_datemath_function_' + ''.join(
            random.choice(string.ascii_lowercase) for _ in range(32)
        )
    else:
        random_prefix = 'curator_get_datemath_function_' + random_element
    datemath_dummy = f'<{random_prefix}-{datemath}>'
    # We both want and expect a 404 here (NotFoundError), since we have
    # created a 32 character random string to definitely be an unknown
    # index name.
    logger.debug('Random datemath string for extraction: %s', datemath_dummy)
    faux_index = ''
    try:
        client.indices.get(index=datemath_dummy)
    except NotFoundError as err:
        # This is the magic.  Elasticsearch still gave us the formatted
        # index name in the error results.
        faux_index = err.body['error']['index']
    logger.debug('Response index name for extraction: %s', faux_index)
    # Now we strip the random index prefix back out again
    # pylint: disable=consider-using-f-string
    pattern = r'^{0}-(.*)$'.format(random_prefix)
    regex = re.compile(pattern)
    try:
        # And return only the now-parsed date string
        return regex.match(faux_index).group(1)
    except AttributeError as exc:
        raise ConfigurationError(
            f'The rendered index "{faux_index}" does not contain a valid date pattern '
            f'or has invalid index name characters.'
        ) from exc


def get_datetime(index_timestamp, timestring):
    """
    :param index_timestamp: The index timestamp
    :param timestring: An ``strftime`` pattern

    :type index_timestamp: str
    :type timestring: :py:func:`~.time.strftime`

    :returns: The datetime extracted from the index name, which is the index creation
        time.
    :rtype: :py:class:`~.datetime.datetime`
    """
    # Compensate for week of year by appending '%w' to the timestring
    # and '1' (Monday) to index_timestamp
    iso_week_number = False
    if '%W' in timestring or '%U' in timestring or '%V' in timestring:
        timestring += '%w'
        index_timestamp += '1'
        if '%V' in timestring and '%G' in timestring:
            iso_week_number = True
            # Fake as so we read Greg format instead. We will process it later
            timestring = timestring.replace("%G", "%Y").replace("%V", "%W")
    elif '%m' in timestring:
        if '%d' not in timestring:
            timestring += '%d'
            index_timestamp += '1'

    mydate = datetime.strptime(index_timestamp, timestring)

    # Handle ISO time string
    if iso_week_number:
        mydate = handle_iso_week_number(mydate, timestring, index_timestamp)

    return mydate


def get_point_of_reference(unit, count, epoch=None):
    """
    :param unit: One of ``seconds``, ``minutes``, ``hours``, ``days``, ``weeks``,
        ``months``, or ``years``.
    :param unit_count: The number of ``units``. ``unit_count`` * ``unit`` will be
        calculated out to the relative number of seconds.
    :param epoch: An epoch timestamp used in conjunction with ``unit`` and
        ``unit_count`` to establish a point of reference for calculations.

    :type unit: str
    :type unit_count: int
    :type epoch: int

    :returns: A point-of-reference timestamp in epoch + milliseconds by deriving from a
        ``unit`` and a ``count``, and an optional reference timestamp, ``epoch``
    :rtype: int
    """
    if unit == 'seconds':
        multiplier = 1
    elif unit == 'minutes':
        multiplier = 60
    elif unit == 'hours':
        multiplier = 3600
    elif unit == 'days':
        multiplier = 3600 * 24
    elif unit == 'weeks':
        multiplier = 3600 * 24 * 7
    elif unit == 'months':
        multiplier = 3600 * 24 * 30
    elif unit == 'years':
        multiplier = 3600 * 24 * 365
    else:
        raise ValueError(f'Invalid unit: {unit}.')
    # Use this moment as a reference point, if one is not provided.
    if not epoch:
        epoch = time.time()
    epoch = fix_epoch(epoch)
    return epoch - multiplier * count


def get_unit_count_from_name(index_name, pattern):
    """
    :param index_name: An index name
    :param pattern: A regular expression pattern

    :type index_name: str
    :type pattern: str

    :returns: The unit count, if a match is able to be found in the name
    :rtype: int
    """
    if pattern is None:
        return None
    match = pattern.search(index_name)
    if match:
        try:
            return int(match.group(1))
        # pylint: disable=broad-except
        except Exception:
            return None
    else:
        return None


def handle_iso_week_number(mydate, timestring, index_timestamp):
    """
    :param mydate: A Python datetime
    :param timestring: An ``strftime`` pattern
    :param index_timestamp: The index timestamp

    :type mydate: :py:class:`~.datetime.datetime`
    :type timestring: :py:func:`~.time.strftime`
    :type index_timestamp: str

    :returns: The date of the previous week based on ISO week number
    :rtype: :py:class:`~.datetime.datetime`
    """
    date_iso = mydate.isocalendar()
    # iso_week_str = "{Y:04d}{W:02d}".format(Y=date_iso[0], W=date_iso[1])
    iso_week_str = f'{date_iso[0]:04d}{date_iso[1]:02d}'
    greg_week_str = datetime.strftime(mydate, "%Y%W")

    # Edge case 1: ISO week number is bigger than Greg week number.
    # Ex: year 2014, all ISO week numbers were 1 more than in Greg.
    if (
        iso_week_str > greg_week_str
        or
        # Edge case 2: 2010-01-01 in ISO: 2009.W53, in Greg: 2010.W00
        # For Greg converting 2009.W53 gives 2010-01-04, converting back
        # to same timestring gives: 2010.W01.
        datetime.strftime(mydate, timestring) != index_timestamp
    ):

        # Remove one week in this case
        mydate = mydate - timedelta(days=7)
    return mydate


def isdatemath(data):
    """
    :param data: An expression to validate as being datemath or not
    :type data: str

    :returns: ``True`` if ``data`` is a valid datemath expression, else ``False``
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    initial_check = r'^(.).*(.)$'
    regex = re.compile(initial_check)
    opener = regex.match(data).group(1)
    closer = regex.match(data).group(2)
    logger.debug('opener =  %s, closer = %s', opener, closer)
    if (opener == '<' and closer != '>') or (opener != '<' and closer == '>'):
        raise ConfigurationError('Incomplete datemath encapsulation in "< >"')
    if opener != '<' and closer != '>':
        return False
    return True


def parse_date_pattern(name):
    """
    Scan and parse ``name`` for :py:func:`~.time.strftime` strings, replacing them with
    the associated value when found, but otherwise returning lowercase values, as
    uppercase snapshot names are not allowed. It will detect if the first character is
    a ``<``, which would indicate ``name`` is going to be using Elasticsearch date math
    syntax, and skip accordingly.

    The :py:func:`~.time.strftime` identifiers that Curator currently recognizes as
    acceptable include:

    * ``Y``: A 4 digit year
    * ``y``: A 2 digit year
    * ``m``: The 2 digit month
    * ``W``: The 2 digit week of the year
    * ``d``: The 2 digit day of the month
    * ``H``: The 2 digit hour of the day, in 24 hour notation
    * ``M``: The 2 digit minute of the hour
    * ``S``: The 2 digit number of second of the minute
    * ``j``: The 3 digit day of the year

    :param name: A name, which can contain :py:func:`~.time.strftime` strings
    :type name: str

    :returns: The parsed date pattern
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    prev, rendered = ('', '')
    logger.debug('Provided index name: %s', name)
    for idx, char in enumerate(name):
        logger.debug('Current character in provided name: %s, position: %s', char, idx)
        if char == '<':
            logger.info('"%s" is probably using Elasticsearch date math.', name)
            rendered = name
            break
        if char == '%':
            pass
        elif char in date_regex() and prev == '%':
            rendered += str(datetime.now(timezone.utc).strftime(f'%{char}'))
        else:
            rendered += char
        logger.debug('Partially rendered name: %s', rendered)
        prev = char
    logger.debug('Fully rendered name: %s', rendered)
    return rendered


def parse_datemath(client, value):
    """
    Validate that ``value`` looks like proper datemath. If it passes this test, then
    try to ship it to Elasticsearch for real. It may yet fail this test, and if it
    does, it will raise a :py:exc:`~.curator.exceptions.ConfigurationError` exception.
    If it passes, return the fully parsed string.

    :param client: A client connection object
    :param value: A string to check for datemath

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type value: str

    :returns: A datemath indexname, fully rendered by Elasticsearch
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    if not isdatemath(value):
        return value
    # if we didn't return here, we can continue, no 'else' needed.
    logger.debug('Properly encapsulated, proceeding to next evaluation...')
    # Our pattern has 4 capture groups.
    # 1. Everything after the initial '<' up to the first '{', which we call ``prefix``
    # 2. Everything between the outermost '{' and '}', which we call ``datemath``
    # 3. An optional inner '{' and '}' containing a date formatter and potentially a
    #    timezone. Not captured.
    # 4. Everything after the last '}' up to the closing '>'
    pattern = r'^<([^\{\}]*)?(\{.*(\{.*\})?\})([^\{\}]*)?>$'
    regex = re.compile(pattern)
    try:
        prefix = regex.match(value).group(1) or ''
        datemath = regex.match(value).group(2)
        # formatter = regex.match(value).group(3) or '' (not captured, but counted)
        suffix = regex.match(value).group(4) or ''
    except AttributeError as exc:
        raise ConfigurationError(
            f'Value "{value}" does not contain a valid datemath pattern.'
        ) from exc

    return f'{prefix}{get_datemath(client, datemath)}{suffix}'
