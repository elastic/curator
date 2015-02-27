## Methods used for filtering indices

from .utils import *

import logging
import time
import re
from datetime import timedelta, datetime, date

logger = logging.getLogger(__name__)

DATE_REGEX = {
    'Y' : '4',
    'y' : '2',
    'm' : '2',
    'W' : '2',
    'U' : '2',
    'd' : '2',
    'H' : '2',
}

def regex_iterate(
    indices, pattern, groupname=None, object_type='index', timestring=None,
    time_unit='days', method=None, value=None, utc_now=None):
    """Iterate over all indices in the list and return a list of matches

    :arg indices: A list of indices to act on
    :arg pattern: A regular expression to iterate all indices against
    :arg groupname: The name of a named capture in pattern.  Currently only acts
        on 'date'
    :arg object_type: Either 'index' or 'snapshot'
    :arg timestring: An strftime string to match the datestamp in an index name.
        Only used for time-based filtering.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``. Only used for time-based filtering.
    :arg method: Either 'older_than' or 'newer_than'. Only used for time-based
        filtering.
    :arg value: Number of ``time_unit``s used to calculate time window. Only
        used for time-based filtering.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    result = []
    indices = ensure_list(indices)
    p = re.compile(pattern)
    for index in indices:
        match = False
        if groupname:
            m = p.search(index)
            if m:
                if m.group(groupname):
                    if groupname == "date":
                        timestamp = m.group(groupname)
                        # Get a boolean result
                        match = timestamp_check(
                            timestamp, timestring=timestring,
                            time_unit=time_unit, method=method,
                            value=value, object_type=object_type,
                            utc_now=utc_now,
                            )
        else:
            m = p.match(index)
            if m:
                match = True
        if match == True:
            result.append(index)
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

def get_index_time(index_timestamp, timestring):
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
    logger.debug("index_timestamp: {0}, timestring: {1}, return value: {2}".format(index_timestamp, timestring, datetime.strptime(index_timestamp, timestring)))
    return datetime.strptime(index_timestamp, timestring)

def get_target_month(month_count, utc_now=None):
    """
    Return datetime object for # of *full* months older than ``month_count`` from
    now, or ``utc_now``, if provided.

    :arg month_count: Number of *full* months
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: Datetime object
    """
    utc_now = date(utc_now.year, utc_now.month, 1) if utc_now else date.today()
    target_date = date(utc_now.year, utc_now.month, 1)

    if month_count < 0:
        for i in range(0, month_count, -1):
            if target_date.month == 12:
                target_date = date(target_date.year+1, 1, 1)
            else:
                target_date = date(target_date.year, target_date.month+1, 1)
    else:
        for i in range(0, month_count):
            if target_date.month == 1:
                target_date = date(target_date.year-1, 12, 1)
            else:
                target_date = date(target_date.year, target_date.month-1, 1)
    return datetime(target_date.year, target_date.month, target_date.day)

def get_cutoff(unit_count=None, time_unit='days', utc_now=None):
    """
    Find the cutoff time based on ``unit_count`` and ``time_unit``.

    :arg unit_count: ``time_unit`` multiplier
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: Datetime object
    """
    if not unit_count:
        logger.error("Missing value for unit_count.")
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
        utc_now = get_index_time(weeknow, '%Y-%W')
    if time_unit == 'months':
        utc_now = utc_now.replace(hour=0)
        cutoff = get_target_month(unit_count, utc_now=utc_now)
    else:
        # This cutoff must be a multiple of time_units
        if unit_count < 0:
            cutoff = utc_now - timedelta(**{time_unit: (unit_count)})
        else:
            cutoff = utc_now - timedelta(**{time_unit: (unit_count - 1)})
    logger.debug("time_cutoff: {0}".format(cutoff))
    return cutoff

def timestamp_check(timestamp, timestring=None, time_unit='days',
                    method='older_than', value=None, utc_now=None, **kwargs):
    """
    Check ``timestamp`` to see if it is ``value`` ``time_unit``s
    ``method`` (older_than or newer_than) the calculated cutoff.

    :arg timestamp: An strftime parsable date string.
    :arg timestring: An strftime string to match against timestamp.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``
    :arg method: Whether the timestamp will be ``older_than`` or ``newer_than``
        the indicated number of whole ``time_units`` will be operated on.
    :arg value: Number of ``time_unit``s used to calculate time window
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: Boolean
    """
    object_type = kwargs['object_type'] if 'object_type' in kwargs else 'index'
    cutoff = get_cutoff(unit_count=value, time_unit=time_unit, utc_now=utc_now)

    if object_type == 'index':
        try:
            object_time = get_index_time(timestamp, timestring)
        except ValueError:
            logger.error('Could not extract a timestamp matching {0} from timestring {1}'.format(timestamp, timestring))
            return False
    elif object_type == 'snapshot':
        try:
            object_time = datetime.utcfromtimestamp(float(timestamp)/1000.0)
        except AttributeError as e:
            logger.debug('Unable to compare time from snapshot {0}.  Error: {1}'.format(object_name, e))
            return False
    else:
        # This should not happen.  This is an error case.
        logger.error("object_type is neither 'index' nor 'snapshot'.")
        return

    if method == "older_than":
        if object_time < cutoff:
            return True
    elif method == "newer_than":
        if object_time > cutoff:
            return True
    else:
        logger.info('Timestamp "{0}" is within the threshold period ({1} {2}).'.format(timestamp, value, time_unit))
    # If we've made it here, we failed.
    return False

def filter_by_space(client, indices, disk_space=None, reverse=True):
    """
    Remove indices from the provided list of indices based on space consumed,
    sorted reverse-alphabetically, by default.  If you set `reverse` to false,
    it will be sorted alphabetically.

    With the default reverse sorting, if only one kind of index is provided--for
    example, indices matching logstash-%Y.%m.%d--then alphabetically will mean
    the oldest get removed first, because lower numbers in the dates mean older
    indices.

    By setting reverse=False, then index3 will be deleted before index2, which
    will be deleted before index1

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg disk_space: Filter indices over *n* gigabytes, alphabetically sorted.
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
    # See https://github.com/elasticsearch/curator/issues/254
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

# def filter_by_space(client, disk_space=2097152.0, prefix='logstash-', suffix='',
#                     exclude_pattern=None, **kwargs):
#     """
#     Yield a list of indices to delete based on space consumed, starting with
#     the oldest.
#
#     :arg client: The Elasticsearch client connection
#     :arg disk_space: Delete indices over *n* gigabytes, starting from the
#         oldest indices.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :rtype: generator object (list of strings)
#     """
#
#     disk_usage = 0.0
#     disk_limit = disk_space * 2**30
#
#     # Use of exclude_pattern here could be _very_ important if you don't
#     # want an index pruned even if it is old.
#     exclude_pattern = kwargs['exclude_pattern'] if 'exclude_pattern' in kwargs else ''
#
#     # These two lines allow us to use common filtering by regex before
#     # gathering stats.  However, there are still pitfalls.  You may still
#     # wind up deleting more of one kind of index than another if you have
#     # multiple kinds.  Also, it still won't work on closed indices, so we
#     # must filter them out.
#     all_indices = get_indices(client, prefix=prefix, suffix=suffix, exclude_pattern=exclude_pattern)
#     not_closed = [i for i in all_indices if not index_closed(client, i)]
#     # Because we're building a csv list of indices to pass, we need to ensure
#     # that we actually have at least one index before creating `csv_indices`
#     # as an empty variable.
#     #
#     # If csv_indices is empty, it will match _all indices, which is bad.
#     # See https://github.com/elasticsearch/curator/issues/254
#     logger.debug('List of indices found: {0}'.format(not_closed))
#     if not_closed:
#         csv_indices = ','.join(not_closed)
#
#         stats = client.indices.status(index=csv_indices)
#
#         sorted_indices = sorted(
#             (
#                 (index_name, index_stats['index']['primary_size_in_bytes'])
#                 for (index_name, index_stats) in stats['indices'].items()
#             ),
#             reverse=True
#         )
#
#         for index_name, index_size in sorted_indices:
#             disk_usage += index_size
#
#             if disk_usage > disk_limit:
#                 yield index_name
#             else:
#                 logger.info('skipping {0}, summed disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))
#     else:
#         logger.warn('No indices found matching provided parameters!')

# def filter_by_timestamp(object_list=[], timestring=None, time_unit='days',
#                         older_than=999999, prefix='logstash-', suffix='',
#                         snapshot_prefix='curator-', utc_now=None, **kwargs):
#     """
#     Pass in a list of indices or snapshots. Return a list of objects older
#     than *n* ``time_unit``\s matching ``prefix``, ``timestring``, and
#     ``suffix``.
#
#     :arg object_list: A list of indices or snapshots
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg snapshot_prefix: Override the default with this value. Defaults to
#         ``curator-``
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     :rtype: generator object (list of strings)
#     """
#     object_type = kwargs['object_type'] if 'object_type' in kwargs else 'index'
#     if prefix:
#         prefix = '.' + prefix if prefix[0] == '*' else prefix
#     if suffix:
#         suffix = '.' + suffix if suffix[0] == '*' else suffix
#     if snapshot_prefix:
#         snapshot_prefix = '.' + snapshot_prefix if snapshot_prefix[0] == '*' else snapshot_prefix
#     dateregex = get_date_regex(timestring)
#     if object_type == 'index':
#         regex = "^" + prefix + "(" + dateregex + ")" + suffix + "$"
#     elif object_type == 'snapshot':
#         regex = "(" + "^" + snapshot_prefix + '.*' + ")"
#
#     cutoff = get_cutoff(older_than=older_than, time_unit=time_unit, utc_now=utc_now)
#
#     for object_name in object_list:
#         retval = object_name
#         if object_type == 'index':
#             try:
#                 index_timestamp = re.search(regex, object_name).group(1)
#             except AttributeError as e:
#                 logger.debug('Unable to match {0} with regular expression {1}.  Error: {2}'.format(object_name, regex, e))
#                 continue
#             try:
#                 object_time = get_index_time(index_timestamp, timestring)
#             except ValueError:
#                 logger.error('Could not find a valid timestamp for {0} with timestring {1}'.format(object_name, timestring))
#                 continue
#         elif object_type == 'snapshot':
#             try:
#                 retval = re.search(regex, object_name['snapshot']).group(1)
#             except AttributeError as e:
#                 logger.debug('Unable to match {0} with regular expression {1}.  Error: {2}'.format(retval, regex, e))
#                 continue
#             try:
#                 object_time = datetime.utcfromtimestamp(object_name['start_time_in_millis']/1000.0)
#             except AttributeError as e:
#                 logger.debug('Unable to compare time from snapshot {0}.  Error: {1}'.format(object_name, e))
#                 continue
#             # if the index is older than the cutoff
#         if object_time < cutoff:
#             yield retval
#         else:
#             logger.info('{0} is within the threshold period ({1} {2}).'.format(retval, older_than, time_unit))
#
# ## By space
# def filter_by_space(client, disk_space=2097152.0, prefix='logstash-', suffix='',
#                     exclude_pattern=None, **kwargs):
#     """
#     Yield a list of indices to delete based on space consumed, starting with
#     the oldest.
#
#     :arg client: The Elasticsearch client connection
#     :arg disk_space: Delete indices over *n* gigabytes, starting from the
#         oldest indices.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :rtype: generator object (list of strings)
#     """
#
#     disk_usage = 0.0
#     disk_limit = disk_space * 2**30
#
#     # Use of exclude_pattern here could be _very_ important if you don't
#     # want an index pruned even if it is old.
#     exclude_pattern = kwargs['exclude_pattern'] if 'exclude_pattern' in kwargs else ''
#
#     # These two lines allow us to use common filtering by regex before
#     # gathering stats.  However, there are still pitfalls.  You may still
#     # wind up deleting more of one kind of index than another if you have
#     # multiple kinds.  Also, it still won't work on closed indices, so we
#     # must filter them out.
#     all_indices = get_indices(client, prefix=prefix, suffix=suffix, exclude_pattern=exclude_pattern)
#     not_closed = [i for i in all_indices if not index_closed(client, i)]
#     # Because we're building a csv list of indices to pass, we need to ensure
#     # that we actually have at least one index before creating `csv_indices`
#     # as an empty variable.
#     #
#     # If csv_indices is empty, it will match _all indices, which is bad.
#     # See https://github.com/elasticsearch/curator/issues/254
#     logger.debug('List of indices found: {0}'.format(not_closed))
#     if not_closed:
#         csv_indices = ','.join(not_closed)
#
#         stats = client.indices.status(index=csv_indices)
#
#         sorted_indices = sorted(
#             (
#                 (index_name, index_stats['index']['primary_size_in_bytes'])
#                 for (index_name, index_stats) in stats['indices'].items()
#             ),
#             reverse=True
#         )
#
#         for index_name, index_size in sorted_indices:
#             disk_usage += index_size
#
#             if disk_usage > disk_limit:
#                 yield index_name
#             else:
#                 logger.info('skipping {0}, summed disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))
#     else:
#         logger.warn('No indices found matching provided parameters!')
