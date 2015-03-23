from .utils import *
from datetime import timedelta, datetime, date
import time
import re
import logging
logger = logging.getLogger(__name__)

REGEX_MAP = {
    'timestring': r'^.*{0}.*$',
    'newer_than': r'(?P<date>{0})',
    'older_than': r'(?P<date>{0})',
    'prefix': r'^{0}.*$',
    'suffix': r'^.*{0}$',
}

DATE_REGEX = {
    'Y' : '4',
    'y' : '2',
    'm' : '2',
    'W' : '2',
    'U' : '2',
    'd' : '2',
    'H' : '2',
    'M' : '2',
    'S' : '2',
}

def build_filter(
        kindOf=None, time_unit=None, timestring=None,
        groupname='date', value=None, exclude=False,
    ):
    """
    Return a filter object based on the arguments.

    :arg kindOf: Can be one of:
        [older_than|newer_than|suffix|prefix|regex|timestring].
        This option defines what kind of filter you will be building.
    :arg exclude: If `True`, exclude matches rather than include
    :arg groupname: The name of a named capture in pattern.  Currently only acts
        on 'date'
    :arg timestring: An strftime string to match the datestamp in an index name.
        Only used for time-based filtering.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.
        (default: ``days``). Only used for time-based filtering.
    :arg value: Depends on `kindOf`. It's a time-unit multiplier for
        `older_than` and `newer_than`.  It is the strftime string if `kindOf` is
        `timestring`. It's used to build the regular expression for other kinds.
    """
    if kindOf not in [  'older_than', 'newer_than', 'regex',
                    'exclude', 'prefix', 'suffix', 'timestring'   ]:
        logger.error('{0}: Invalid value for kindOf'.format(kindOf))
        return {}
    # Stop here if None or empty value, but zero is okay
    if value == 0:
        logger.info("I found a zero value")
        argdict = {}
    elif not value:
        return {}
    else:
        argdict = {}

    if kindOf in ['older_than', 'newer_than']:
        if not time_unit:
            logger.error("older_than and newer_than require time_unit parameter")
            return {}
        if not timestring:
            logger.error("older_than and newer_than require timestring parameter")
            return {}
        argdict = {  "groupname":groupname, "time_unit":time_unit,
                    "timestring": timestring, "value": value,
                    "method": kindOf }
        date_regex = get_date_regex(timestring)
        regex = REGEX_MAP[kindOf].format(date_regex)
    elif kindOf == 'timestring':
        regex = r'^.*{0}.*$'.format(get_date_regex(value))
    elif kindOf == 'regex':
        regex = r'{0}'.format(value)
    elif kindOf in ['prefix', 'suffix']:
        regex = REGEX_MAP[kindOf].format(value)

    if kindOf == 'exclude':
        regex = r'{0}'.format(value)
        argdict['exclude'] = True

    logger.debug("REGEX = {0}".format(regex))
    argdict['pattern'] = regex
    logger.debug("Added filter: {0}".format(argdict))
    return argdict

def apply_filter(
    items, pattern=None, exclude=False, groupname=None, timestring=None,
    time_unit=None, method=None, value=None, utc_now=None):
    """Iterate over all items in the list and return a list of matches

    :arg items: A list of indices or snapshots to act on
    :arg pattern: A regular expression to iterate all indices against.
    :arg exclude: If `True`, exclude matches rather than include
    :arg groupname: The name of a named capture in pattern.  Currently only acts
        on 'date'
    :arg timestring: An strftime string to match the datestamp in an index name.
        Only used for time-based filtering.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.
        (default: ``days``). Only used for time-based filtering.
    :arg method: Either ``older_than`` or ``newer_than``. Only used for
        time-based filtering.
    :arg value: `time_unit` multiplier used to calculate time window. Only
        used for time-based filtering.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    if not pattern:
        logger.error("Missing required pattern parameter.")
        return None
    p = re.compile(pattern)
    if exclude:
        return list(filter(lambda x: not p.search(x), items))
    result = []
    items = ensure_list(items)
    for item in items:
        match = False
        if groupname:
            m = p.search(item)
            if m:
                if m.group(groupname):
                    if groupname == "date":
                        timestamp = m.group(groupname)
                        # Get a boolean result
                        match = timestamp_check(
                            timestamp, timestring=timestring,
                            time_unit=time_unit, method=method,
                            value=value, utc_now=utc_now,
                            )
        else:
            m = p.match(item)
            if m:
                match = True
        if match == True:
            result.append(item)
    return result

def get_date_regex(timestring):
    """
    Return a regex string based on a provided strftime timestring.

    :arg timestring: An strftime pattern
    :rtype: str
    """
    prev = ''; curr = ''; regex = ''
    for s in range(0, len(timestring)):
        curr = timestring[s]
        if curr == '%':
            pass
        elif curr in DATE_REGEX and prev == '%':
            regex += '\d{' + DATE_REGEX[curr] + '}'
        else:
            regex += "\\" + curr
        prev = curr
    logger.debug("regex = {0}".format(regex))
    return regex

def get_datetime(index_timestamp, timestring):
    """
    Return the datetime extracted from the index name, which is the index
    creation time.

    :arg index_timestamp: The timestamp extracted from an index name
    :arg timestring: An strftime pattern
    :rtype: Datetime object
    """
    # Compensate for week of year by appending '%w' to the timestring
    # and '1' (Monday) to index_timestamp
    if '%W' in timestring:
        timestring += '%w'
        index_timestamp += '1'
    elif '%U' in timestring:
        timestring += '%w'
        index_timestamp += '1'
    elif '%m' in timestring:
        if not '%d' in timestring:
            timestring += '%d'
            index_timestamp += '1'
    #logger.debug("index_timestamp: {0}, timestring: {1}, return value: {2}".format(index_timestamp, timestring, datetime.strptime(index_timestamp, timestring)))
    return datetime.strptime(index_timestamp, timestring)

def month_bump(datestamp, sign='positive'):
    """
    This method returns either the next or previous month based on the value of
    sign.

    :arg datestamp: A datetime datestamp
    :arg sign: Either `positive` or `negative`
    :rtype: Datetime object
    """
    if sign is not 'positive' and sign is not 'negative':
        raise ValueError
    if sign == 'positive':
        if datestamp.month == 1:
            return date(datestamp.year-1, 12, 1)
        else:
            return date(datestamp.year, datestamp.month-1, 1)
    else:
        if datestamp.month == 12:
            return date(datestamp.year+1, 1, 1)
        else:
            return date(datestamp.year, datestamp.month+1, 1)

def get_target_month(month_count, utc_now=None):
    """
    Return datetime object for number of *full* months older than
    `month_count` from now, or `utc_now`, if provided.

    :arg month_count: Number of *full* months
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: Datetime object
    """
    utc_now = date(utc_now.year, utc_now.month, 1) if utc_now else date.today()
    target_date = date(utc_now.year, utc_now.month, 1)

    if month_count < 0:
        for i in range(0, month_count, -1):
            target_date = month_bump(target_date, sign='negative')
    elif month_count > 0:
        for i in range(0, month_count):
            target_date = month_bump(target_date)
    return datetime(target_date.year, target_date.month, target_date.day)

def get_cutoff(unit_count=None, time_unit='days', utc_now=None):
    """
    Find the cutoff time based on `unit_count` and `time_unit`.

    :arg unit_count: `time_unit` multiplier
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``. (default:
        ``days``)
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: Datetime object
    """
    if unit_count == None:
        logger.error("Missing value for unit_count.")
        return False
    if type(unit_count) != type(int()):
        logger.error("Non-integer value for unit_count.")
        return False
    # time-injection for test purposes only
    utc_now = utc_now if utc_now else datetime.utcnow()
    # reset to start of the period to be sure we are not retiring a human by mistake
    utc_now = utc_now.replace(minute=0, second=0, microsecond=0)

    if time_unit == 'days':
        utc_now = utc_now.replace(hour=0)
    if time_unit == 'weeks':
        # Since week math always uses Monday as the start of the week,
        # this work-around resets utc_now to be Monday of the current week.
        weeknow = utc_now.strftime('%Y-%W')
        utc_now = get_datetime(weeknow, '%Y-%W')
    if time_unit == 'months':
        utc_now = utc_now.replace(hour=0)
        cutoff = get_target_month(unit_count, utc_now=utc_now)
    else:
        # This cutoff must be a multiple of time_units
        if unit_count < 0:
            cutoff = utc_now - timedelta(**{time_unit: (unit_count)})
        else:
            cutoff = utc_now - timedelta(**{time_unit: (unit_count - 1)})
    #logger.debug("time_cutoff: {0}".format(cutoff))
    return cutoff

def timestamp_check(timestamp, timestring=None, time_unit=None,
                    method='older_than', value=None, utc_now=None):
    """
    Check `timestamp` to see if it is `value` * `time_unit`
    `method` (``older_than`` or ``newer_than``) the calculated cutoff.

    :arg timestamp: An strftime parsable date string.
    :arg timestring: An strftime string to match against ``timestamp``.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.
    :arg method: ``older_than`` or ``newer_than``.
    :arg value: `time_unit` multiplier used to calculate time window.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: bool
    """
    cutoff = get_cutoff(unit_count=value, time_unit=time_unit, utc_now=utc_now)

    try:
        object_time = get_datetime(timestamp, timestring)
    except ValueError:
        logger.error('Could not extract a timestamp matching {0} from timestring {1}'.format(timestamp, timestring))
        return False

    if method == "older_than":
        if object_time < cutoff:
            return True
    elif method == "newer_than":
        if object_time > cutoff:
            return True

    logger.debug('Timestamp "{0}" is outside the cutoff period ({1} {2} {3}).'.format(
                        timestamp, method.replace('_', ' '), value, time_unit))
    return False

def filter_by_space(client, indices, disk_space=None, reverse=True):
    """
    Remove indices from the provided list of indices based on space consumed,
    sorted reverse-alphabetically by default.  If you set `reverse` to
    `False`, it will be sorted alphabetically.

    The default is usually what you will want. If only one kind of index is
    provided--for example, indices matching ``logstash-%Y.%m.%d``--then reverse
    alphabetical sorting will mean the oldest get removed first, because lower
    numbers in the dates mean older indices.

    By setting reverse to `False`, then ``index3`` will be deleted before
    ``index2``, which will be deleted before ``index1``

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg disk_space: Filter indices over *n* gigabytes
    :arg reverse: The filtering direction. (default: `True`)
    :rtype: list
    """

    if not disk_space:
        logger.error("Mising value for disk_space.")
        return False

    disk_usage = 0.0
    disk_limit = disk_space * 2**30
    delete_list = []

    not_closed = [i for i in indices if not index_closed(client, i)]
    # Because we're building a csv list of indices to pass, we need to ensure
    # that we actually have at least one index before calling
    # client.indices.status, otherwise the call will match _all indices, which
    # is very bad.
    # See https://github.com/elastic/curator/issues/254
    logger.debug('List of indices found: {0}'.format(not_closed))
    if not_closed:

        stats = client.indices.status(index=to_csv(not_closed))

        sorted_indices = sorted(
            (
                (index_name, index_stats['index']['primary_size_in_bytes'])
                for (index_name, index_stats) in stats['indices'].items()
            ),
            reverse=reverse
        )

        for index_name, index_size in sorted_indices:
            disk_usage += index_size

            if disk_usage > disk_limit:
                delete_list.append(index_name)
            else:
                logger.info('skipping {0}, summed disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))
    return delete_list
