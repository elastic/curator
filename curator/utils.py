import time
import re
from datetime import timedelta, datetime, date

def ensure_list(indices):
    """
    Return a list, even if indices is a single value

    :arg indices: A list of indices to act upon
    :rtype: list
    """
    if type(indices) is not type(list()):   # in case of a single value passed
        indices = [indices]
    return indices

def to_csv(indices):
    """
    Return a csv string from a list of indices, or a single value if only one
    value is present

    :arg indices: A list of indices to act on, or a single value, which could be
        in the format of a csv string already.
    :rtype: str
    """
    indices = ensure_list(indices) # in case of a single value passed
    if len(indices) > 1:
        return ','.join(sorted(indices))
    else:
        return indices[0]

def check_csv(value):
    """
    Some of the curator methods should not operate against multiple indices at
    once.  This method can be used to check if a list or csv has been sent.

    :arg value: The value to test, if list or csv string
    :rtype: bool
    """
    if type(value) is type(list()):
        return True
    else:
        if len(value.split(',')) > 1: # It's a csv string.
            return True
        else: # There's only one value here, so it's not a csv string
            return False

def prune_kibana(indices):
    """Remove any index named .kibana, kibana-int, or .marvel-kibana

    :arg indices: A list of indices to act upon.
    """
    indices = ensure_list(indices)
    if '.marvel-kibana' in indices:
        indices.remove('.marvel-kibana')
    if 'kibana-int' in indices:
        indices.remove('kibana-int')
    if '.kibana' in indices:
        indices.remove('.kibana')
    return indices

def regex_iterate(
    indices, pattern, groupname=None, object_type='index', timestring=None,
    time_unit='days', method=None, value=None):
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
                            value=value, object_type=object_type
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

    for i in range(0, month_count):
        if target_date.month == 1:
            target_date = date(target_date.year-1, 12, 1)
        else:
            target_date = date(target_date.year, target_date.month-1, 1)

    return datetime(target_date.year, target_date.month, target_date.day)

def time_cutoff(unit_count=None, time_unit='days', utc_now=None):
    """
    Find the cutoff time based on ``unit_count`` and ``time_unit``.

    :arg unit_count: ``time_unit`` multiplier
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: Datetime object
    """
    if not unit_count:
        logger.error("No value specified for unit_count.")
        return
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
        cutoff = utc_now - timedelta(**{time_unit: (unit_count - 1)})
    return cutoff

def get_cutoff(older_than=999999, time_unit='days', utc_now=None):
    """
    Find the cutoff time based on ``older_than`` and ``time_unit``.

    :arg older_than: ``time_unit`` multiplier
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: Datetime object
    """
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
        cutoff = get_target_month(older_than, utc_now=utc_now)
    else:
        # This cutoff must be a multiple of time_units
        cutoff = utc_now - timedelta(**{time_unit: (older_than - 1)})
    return cutoff

def index_closed(client, index_name):
    """
    Return `True` if the indicated index is closed.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :rtype: bool
    """
    index_metadata = client.cluster.state(
        index=index_name,
        metric='metadata',
    )
    return index_metadata['metadata']['indices'][index_name]['state'] == 'close'

def prune_closed(client, indices):
    """
    Return list of indices that are not closed.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: list
    """
    indices = ensure_list(indices)
    retval = []
    for idx in indices:
        if not index_closed(client, idx):
            retval.append(idx)
        else:
            logger.info('Skipping index {0}: Already closed.'.format(idx))
    return sorted(retval)

## ES version
def get_version(client):
    """
    Return the ES version number as a tuple.
    Omits trailing tags like -dev, or Beta

    :arg client: The Elasticsearch client connection
    :rtype: tuple
    """
    version = client.info()['version']['number']
    if len(version.split('.')) > 3:
        version = version.split('.')[:-1]
    else:
       version = version.split('.')
    return tuple(map(int, version))

## Segment count
def get_segmentcount(client, index_name):
    """
    Return a tuple of ``(shardcount, segmentcount)`` from the provided
    ``index_name``.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :rtype: tuple
    """
    shards = client.indices.segments(index=index_name)['indices'][index_name]['shards']
    segmentcount = 0
    totalshards = 0 # We will increment this manually to capture all replicas...
    for shardnum in shards:
        for shard in range(0,len(shards[shardnum])):
            segmentcount += shards[shardnum][shard]['num_search_segments']
            totalshards += 1
    return totalshards, segmentcount

## Is the current node the elected master?
def is_master_node(client):
    """
    Return `True` if the connected client node is the elected master node in
    the Elasticsearch cluster, otherwise return `False`.

    :arg client: The Elasticsearch client connection
    :rtype: bool
    """
    my_node_id = client.nodes.info('_local')['nodes'].keys()[0]
    master_node_id = client.cluster.state(metric='master_node')['master_node']
    return my_node_id == master_node_id


# Filtering
## By timestamp
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
    cutoff = time_cutoff(unit_count=value, time_unit=time_unit, utc_now=utc_now)

    if object_type == 'index':
        try:
            object_time = get_index_time(timestamp, timestring)
        except ValueError:
            logger.error('Could not find a valid timestamp for timestring {0}'.format(timestring))

    elif object_type == 'snapshot':
        try:
            object_time = datetime.utcfromtimestamp(float(timestamp)/1000.0)
        except AttributeError as e:
            logger.debug('Unable to compare time from snapshot {0}.  Error: {1}'.format(object_name, e))
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


## Loop through a list of objects and perform the indicated operation
# def _op_loop(client, object_list, op=None, dry_run=False, **kwargs):
#     """
#     Perform the ``op`` on indices or snapshots in the ``object_list``
#
#     .. note::
#        All kwargs are passed on to the specified ``op``.
#
#        Any arg the ``op`` needs *must* be passed in **kwargs**.
#
#     :arg client: The Elasticsearch client connection
#     :arg object_list: The list of indices or snapshots
#     :arg op: The operation to perform on each object in ``object_list``
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg delay: Can be specified to pause after each iteration in the loop.
#         Useful to allow cluster to quiesce after heavy I/O optimizations.
#     """
#     if not op:
#         logger.error("No operation specified.")
#         return
#     prepend = kwargs['prepend'] if 'prepend' in kwargs else ''
#     for item in object_list:
#         if dry_run: # Don't act on a dry run
#             logger.info(prepend + "of {0} operation on {1}".format(op.__name__, item))
#             continue
#         skipped = op(client, item, **kwargs)
#         if skipped:
#             continue
#         if 'delay' in kwargs:
#             if kwargs['delay'] > 0:
#                 logger.info('Pausing for {0} seconds to allow cluster to quiesce...'.format(kwargs['delay']))
#                 time.sleep(kwargs['delay'])
#         logger.info("{0} operation succeeded on {1}".format(op.__name__, item))
#
# ## curator alias [ARGS]
# def alias(client, dry_run=False, **kwargs):
#     """
#     Multiple use cases:
#
#     1. Add indices ``alias_older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix`` to ``alias``.
#     2. Remove indices ``unalias_older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix`` to ``alias``.
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.add_to_alias`
#
#        :py:func:`curator.curator.add_to_alias`
#
#     These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg alias: Alias name to operate on.
#     :arg alias_older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg unalias_older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg prefix: A string that comes before the datestamp in an index name. Can
#         be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name. Can
#         be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg exclude_pattern: Exclude indices matching the provided regular expression.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     logging.info(kwargs['prepend'] + "Performing alias operations...")
#     if kwargs['alias_older_than']:
#         kwargs['older_than'] = kwargs['alias_older_than']
#         op = add_to_alias
#     elif kwargs['unalias_older_than']:
#         kwargs['older_than'] = kwargs['unalias_older_than']
#         op = remove_from_alias
#     else:
#         op = None
#     index_list = get_object_list(client, **kwargs)
#     matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
#     _op_loop(client, matching_indices, op=op, dry_run=dry_run, **kwargs)
#     logger.info(kwargs['prepend'] + 'Alias operations completed for specified indices.')
#
# ## curator allocation [ARGS]
# def allocation(client, dry_run=False, **kwargs):
#     """
#     Change shard/routing allocation for indices ``older_than`` *n*
#     ``time_unit``\s, matching the given ``timestring``, ``prefix``, and
#     ``suffix`` by updating/applying the provided ``rule``.
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.apply_allocation_rule`
#
#        These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg rule: The routing allocation rule to apply, e.g. ``tag=ssd``.  Must be
#         in the format of ``key=value``, and should match values declared on the
#         correlating nodes in your cluster.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     logging.info(kwargs['prepend'] + "Applying allocation/routing tags to indices...")
#     index_list = get_object_list(client, **kwargs)
#     matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
#     _op_loop(client, matching_indices, op=apply_allocation_rule, dry_run=dry_run, **kwargs)
#     logger.info(kwargs['prepend'] + 'Allocation/routing tags applied to specified indices.')
#
# ## curator bloom [ARGS]
# def bloom(client, dry_run=False, **kwargs):
#     """
#     Disable bloom filter cache for indices ``older_than`` *n* ``time_unit``\s,
#     matching the given ``timestring``, ``prefix``, and ``suffix``.
#
#     Can optionally ``delay`` a given number of seconds after each optimization.
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.disable_bloom_filter`
#
#        These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :arg delay: Pause *n* seconds after optimizing an index.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     logging.info(kwargs['prepend'] + "Disabling the bloom filter cache for indices...")
#     index_list = get_object_list(client, **kwargs)
#     matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
#     _op_loop(client, matching_indices, op=disable_bloom_filter, dry_run=dry_run, **kwargs)
#     logger.info(kwargs['prepend'] + 'Disabled bloom filter cache for specified indices.')
#
# ## curator close [ARGS]
# def close(client, dry_run=False, **kwargs):
#     """
#     Close indices ``older_than`` *n* ``time_unit``\s, matching the given
#     ``timestring``, ``prefix``, and ``suffix``.
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.close_index`
#
#        These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     logging.info(kwargs['prepend'] + "Closing indices...")
#     index_list = get_object_list(client, **kwargs)
#     matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
#     _op_loop(client, matching_indices, op=close_index, dry_run=dry_run, **kwargs)
#     logger.info(kwargs['prepend'] + 'Closed specified indices.')
#
# ## curator delete [ARGS]
# def delete(client, dry_run=False, **kwargs):
#     """
#     Two use cases for deleting indices:
#
#     1. Delete indices ``older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix``.
#     2. Delete indices in excess of ``disk_space`` gigabytes if the ``disk_space`` kwarg is present, beginning from the oldest.  Indices must still match ``prefix`` and ``suffix``.  This amount spans all nodes and shards and must be calculated accordingly.  This is not a recommended use-case.
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_space`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.delete_index`
#
#        These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg disk_space: Delete indices over *n* gigabytes, starting from the
#         oldest indices.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     logging.info(kwargs['prepend'] + "Deleting indices...")
#     by_space = kwargs['disk_space'] if 'disk_space' in kwargs else False
#     if by_space:
#         logger.info(kwargs['prepend'] + 'Deleting by space rather than time.')
#         matching_indices = filter_by_space(client, **kwargs)
#     else:
#         index_list = get_object_list(client, **kwargs)
#         matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
#     _op_loop(client, matching_indices, op=delete_index, dry_run=dry_run, **kwargs)
#     logger.info(kwargs['prepend'] + 'Specified indices deleted.')
#
# ## curator optimize [ARGS]
# def optimize(client, dry_run=False, **kwargs):
#     """
#     Optimize indices ``older_than`` *n* ``time_unit``\s, matching the given
#     ``timestring``, ``prefix``, and ``suffix`` to ``max_num_segments`` segments
#     per shard.
#
#     Can optionally ``delay`` a given number of seconds after each optimization.
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.optimize_index`
#
#        These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If True, simulate, but do not perform the operation
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg max_num_segments: Merge to this number of segments per shard.
#     :arg delay: Pause *n* seconds after optimizing an index.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     logging.info(kwargs['prepend'] + "Optimizing indices...")
#     index_list = get_object_list(client, **kwargs)
#     matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
#     _op_loop(client, matching_indices, op=optimize_index, dry_run=dry_run, **kwargs)
#     logger.info(kwargs['prepend'] + 'Optimized specified indices.')
#
# ## curator (change) replicas [ARGS]
# def replicas(client, dry_run=False, **kwargs):
#     """
#     Change replica count for indices ``older_than`` *n* ``time_unit``\s,
#     matching the given ``timestring``, ``prefix``, and ``suffix``.
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.close_index`
#
#        These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg replicas: The number of replicas the index should have
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_units`` will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     logging.info(kwargs['prepend'] + "Changing replica count of indices...")
#     index_list = get_object_list(client, **kwargs)
#     matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
#     _op_loop(client, matching_indices, op=change_replicas, dry_run=dry_run, **kwargs)
#     logger.info(kwargs['prepend'] + 'Changing replica count for specified indices.')
#
# ## curator snapshot [ARGS]
# def snapshot(client, dry_run=False, **kwargs):
#     """
#     Multiple use cases:
#
#     1. Snapshot indices ``older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix`` into ``repository``.
#     2. Snapshot *n* ``most_recent`` indices matching ``prefix`` and ``suffix`` into ``repository``.
#     3. ``delete_older_than`` *n* ``time_units`` matching the given ``timestring``, ``prefix``, and ``suffix`` from ``repository``.
#     4. Snapshot ``all_indices`` (ignoring ``older_than`` and ``most_recent``).
#
#     .. note::
#        As this is an iterative function, default values are handled by the
#        target function(s).
#
#        Unless passed in `kwargs`, parameters other than ``client`` and
#        ``dry_run`` will have default values assigned by the functions being
#        called:
#
#        :py:func:`curator.curator.get_object_list`
#
#        :py:func:`curator.curator.filter_by_timestamp`
#
#        :py:func:`curator.curator.create_snapshot`
#
#        :py:func:`curator.curator.delete_snapshot`
#
#        These defaults are included here for documentation.
#
#     :arg client: The Elasticsearch client connection
#     :arg dry_run: If true, simulate, but do not perform the operation
#     :arg older_than: Indices older than the indicated number of whole
#         ``time_unit``\s will be operated on.
#     :arg most_recent: Most recent *n* indices will be operated on.
#     :arg all_indices: Boolean.  Include ``_all`` indices in snapshot.
#     :type all_indices: bool
#     :arg delete_older_than: Snapshots older than the indicated number of whole
#         ``time_unit``\s will be operated on.
#     :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
#         is ``days``.
#     :arg timestring: An strftime string to match the datestamp in an index name.
#     :arg snapshot_name: Override the default with this value. Defaults to `None`
#     :arg snapshot_prefix: Override the default with this value. Defaults to
#         ``curator-``
#     :arg prefix: A string that comes before the datestamp in an index name.
#         Can be empty. Wildcards acceptable.  Default is ``logstash-``.
#     :arg suffix: A string that comes after the datestamp of an index name.
#         Can be empty. Wildcards acceptable.  Default is empty, ``''``.
#     :arg repository: The Elasticsearch snapshot repository to use
#     :arg wait_for_completion: Wait (or not) for the operation
#         to complete before returning.  Waits by default, i.e. Default is
#         `True`
#     :type wait_for_completion: bool
#     :arg ignore_unavailable: Ignore unavailable shards/indices.
#         Default is `False`
#     :type ignore_unavailable: bool
#     :arg include_global_state: Store cluster global state with snapshot.
#         Default is `True`
#     :type include_global_state: bool
#     :arg partial: Do not fail if primary shard is unavailable. Default
#         is `False`
#     :type partial: bool
#     :arg exclude_pattern: Exclude indices matching the provided regular
#         expression.
#     :arg utc_now: Used for testing.  Overrides current time with specified time.
#     """
#     if not 'repository' in kwargs:
#         logger.error("Repository name not provided.")
#         return
#     kwargs['prepend'] = "DRY RUN: " if dry_run else ''
#     if not 'older_than' in kwargs and not 'most_recent' in kwargs and not 'delete_older_than' in kwargs and not 'all_indices'in kwargs:
#         logger.error('Expect missing argument.')
#         return
#     # Preserving kwargs intact for passing to _op_loop is the game here...
#     all_indices       = kwargs['all_indices'] if 'all_indices' in kwargs else False
#     delete_older_than = kwargs['delete_older_than'] if 'delete_older_than' in kwargs else None
#     older_than        = kwargs['older_than'] if 'older_than' in kwargs else None
#     most_recent       = kwargs['most_recent'] if 'most_recent' in kwargs else None
#     if delete_older_than is not None:
#         logger.info(kwargs['prepend'] + "Deleting specified snapshots...")
#         kwargs['older_than'] = kwargs['delete_older_than'] # Fix for delete in this case only.
#         snapshot_list = client.snapshot.get(repository=kwargs['repository'], snapshot="_all")['snapshots']
#         matching_snapshots = list(filter_by_timestamp(object_list=snapshot_list, object_type='snapshot', **kwargs))
#         _op_loop(client, matching_snapshots, op=delete_snapshot, dry_run=dry_run, **kwargs)
#         logger.info(kwargs['prepend'] + 'Specified snapshots deleted.')
#     else:
#         logger.info(kwargs['prepend'] + "Capturing snapshots of specified indices...")
#         if not all_indices:
#             index_list = get_object_list(client, **kwargs)
#             if most_recent is not None:
#                 matching_indices = index_list[-kwargs['most_recent']:]
#             elif older_than is not None:
#                 matching_indices = list(filter_by_timestamp(object_list=index_list, **kwargs))
#             else:
#                 logger.error(kwargs['prepend'] + 'Missing argument: Must provide one of: older_than, most_recent, all_indices, delete_older_than')
#                 return
#             logger.info(kwargs['prepend'] + 'Snapshot will capture indices: {0}'.format(', '.join(matching_indices)))
#         else:
#             matching_indices = '_all'
#             logger.info(kwargs['prepend'] + 'Snapshot will capture all indices')
#         if not dry_run:
#             # Default `create_snapshot` behavior is to snap `_all` into a
#             # snapshot named `snapshot_name` or `curator-%Y-%m-%dT%H:%M:%S`
#             create_snapshot(client, indices=matching_indices, **kwargs)
#         logger.info(kwargs['prepend'] + 'Snapshots captured for specified indices.')
