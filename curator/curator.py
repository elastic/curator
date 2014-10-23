import time
import logging
import re
from datetime import timedelta, datetime, date

import elasticsearch

__version__ = '2.1.0-dev'

logger = logging.getLogger(__name__)

DATE_REGEX = {
    'Y' : '4',
    'm' : '2',
    'W' : '2',
    'U' : '2',
    'd' : '2',
    'H' : '2',
}

# Information retrieval
## Date & Time
### Date Regex
def get_date_regex(timestring):
    """
    Return a regex string based on a provided strftime timestring.
    
    :arg timestring: An strftime pattern
    :rtype: Regex as string
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

### Index time
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

### Index time if unit = months
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

### Cutoff time
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

## Alias information
def get_alias(client, alias):
    """
    Return information about the specified alias.

    :arg client: The Elasticsearch client connection
    :arg alias: Alias name to operate on.
    :rtype: list of strings
    """
    if client.indices.exists_alias(alias):
        return client.indices.get_alias(name=alias).keys()
    else:
        logger.error('Unable to find alias {0}.'.format(alias))
        return False

## Is the index closed?
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

## Get matching indices
def get_indices(client, prefix='logstash-', suffix='', exclude_pattern=None):
    """
    Return a sorted list of indices matching ``prefix`` and ``suffix`` and
    optionally filtered by ``exclude_pattern``.
    
    :arg client: The Elasticsearch client connection
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :rtype: list of strings
    """
    _indices = sorted(client.indices.get_settings(index='*', params={'expand_wildcards': 'open,closed'}).keys())
    if prefix:
        prefix = '.' + prefix if prefix[0] == '*' else prefix
    if suffix:
        suffix = '.' + suffix if suffix[0] == '*' else suffix
    regex = "^" + prefix + ".*" + suffix + "$"
    _fixes = re.compile(regex)
    _indices = list(filter(lambda x: _fixes.search(x), _indices))
    if '.marvel-kibana' in _indices:
        _indices.remove('.marvel-kibana')
    if 'kibana-int' in _indices:
        _indices.remove('kibana-int')
    if exclude_pattern:
        pattern = re.compile(exclude_pattern)
        return list(filter(lambda x: not pattern.search(x), _indices))
    else:
        return _indices

## Snapshot information
### List of matching snapshots
def get_snaplist(client, repository='', snapshot_prefix='curator-'):
    """
    Get ``_all`` snapshots containing ``snapshot_prefix`` from repository and
    return a list.
    
    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    :rtype: list of strings
    """
    retval = []
    try:
        allsnaps = client.snapshot.get(repository=repository, snapshot="_all")['snapshots']
        snaps = [snap['snapshot'] for snap in allsnaps if 'snapshot' in snap.keys()]
        if snapshot_prefix:
            snapshot_prefix = '.' + snapshot_prefix if snapshot_prefix[0] == '*' else snapshot_prefix
        regex = "^" + snapshot_prefix + ".*" + "$"
        pattern = re.compile(regex)
        return list(filter(lambda x: pattern.search(x), snaps))
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
    return retval

### Repository information
def get_repository(client, repository=''):
    """
    Return configuration information for the indicated repository.

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: dict
    """
    try:
        return client.snapshot.get_repository(repository=repository)
    except elasticsearch.NotFoundError as e:
        logger.info("Repository {0} not found.  Error: {1}".format(repository, e))
        return None

### Single snapshot information
def get_snapshot(client, repository='', snapshot=''):
    """
    Return information about a snapshot (or a comma-separated list of snapshots)

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name, or a comma-separated list of snapshots
    :rtype: dict
    """
    try:
        return client.snapshot.get(repository=repository, snapshot=snapshot)
    except elasticsearch.NotFoundError as e:
        logger.info("Snapshot or repository {0} not found.  Error: {1}".format(snapshot, e))
        return None

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

## Matching indices or snapshots
def get_object_list(client, data_type='index', prefix='logstash-', suffix='', repository=None, snapshot_prefix='curator-', exclude_pattern=None, **kwargs):
    """
    Return a list of indices matching ``prefix`` and ``suffix`` or snapshots
    matching ``snapshot_prefix``.
    
    Can optionally exclude by regular expression pattern with
    ``exclude_pattern``.
    
    :arg client: The Elasticsearch client connection
    :arg data_type: Either ``index`` or ``snapshot``
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg repository: The Elasticsearch snapshot repository to use (only with
        snapshots)
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :rtype: list of strings
    """
    if data_type == 'index':
        object_list = get_indices(client, prefix=prefix, suffix=suffix)
    elif data_type == 'snapshot':
        if repository:
            object_list = get_snaplist(client, repository=repository, snapshot_prefix=snapshot_prefix)
        else:
            logger.error('Repository name not specified. Returning empty list.')
            object_list = []
    else:
        object_list = []
        logger.error('data_type \'{0}\' is neither \'index\' nor \'snapshot\'.  Returning empty list.'.format(data_type))
    if exclude_pattern:
        pattern = re.compile(exclude_pattern)
        return list(filter(lambda x: not pattern.search(x), object_list))
    else:
        return object_list

# Filtering
## By timestamp
def filter_by_timestamp(object_list=[], timestring=None, time_unit='days',
                        older_than=999999, prefix='logstash-', suffix='',
                        snapshot_prefix='curator-', utc_now=None, **kwargs):
    """
    Pass in a list of indices or snapshots. Return a list of objects older
    than *n* ``time_unit``\s matching ``prefix``, ``timestring``, and
    ``suffix``.
    
    :arg object_list: A list of indices or snapshots
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``
    :arg older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    :rtype: generator object (list of strings)
    """
    object_type = kwargs['object_type'] if 'object_type' in kwargs else 'index'
    if prefix:
        prefix = '.' + prefix if prefix[0] == '*' else prefix
    if suffix:
        suffix = '.' + suffix if suffix[0] == '*' else suffix
    if snapshot_prefix:
        snapshot_prefix = '.' + snapshot_prefix if snapshot_prefix[0] == '*' else snapshot_prefix
    dateregex = get_date_regex(timestring)
    if object_type == 'index':
        regex = "^" + prefix + "(" + dateregex + ")" + suffix + "$"
    elif object_type == 'snapshot':
        regex = "(" + "^" + snapshot_prefix + '.*' + ")"

    cutoff = get_cutoff(older_than=older_than, time_unit=time_unit, utc_now=utc_now)
    
    for object_name in object_list:
        retval = object_name
        if object_type == 'index':
            try:
                index_timestamp = re.search(regex, object_name).group(1)
            except AttributeError as e:
                logger.debug('Unable to match {0} with regular expression {1}.  Error: {2}'.format(object_name, regex, e))
                continue
            try:
                object_time = get_index_time(index_timestamp, timestring)
            except ValueError:
                logger.error('Could not find a valid timestamp for {0} with timestring {1}'.format(object_name, timestring))
                continue
        elif object_type == 'snapshot':
            try:
                retval = re.search(regex, object_name['snapshot']).group(1)
            except AttributeError as e:
                logger.debug('Unable to match {0} with regular expression {1}.  Error: {2}'.format(retval, regex, e))
                continue
            try:
                object_time = datetime.utcfromtimestamp(object_name['start_time_in_millis']/1000.0)
            except AttributeError as e:
                logger.debug('Unable to compare time from snapshot {0}.  Error: {1}'.format(object_name, e))
                continue
            # if the index is older than the cutoff
        if object_time < cutoff:
            yield retval
        else:
            logger.info('{0} is within the threshold period ({1} {2}).'.format(retval, older_than, time_unit))

## By space
def filter_by_space(client, disk_space=2097152.0, prefix='logstash-', suffix='',
                    exclude_pattern=None, **kwargs):
    """
    Yield a list of indices to delete based on space consumed, starting with
    the oldest.
    
    :arg client: The Elasticsearch client connection
    :arg disk_space: Delete indices over *n* gigabytes, starting from the
        oldest indices.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :rtype: generator object (list of strings)
    """

    disk_usage = 0.0
    disk_limit = disk_space * 2**30

    # Use of exclude_pattern here could be _very_ important if you don't
    # want an index pruned even if it is old.
    exclude_pattern = kwargs['exclude_pattern'] if 'exclude_pattern' in kwargs else ''

    # These two lines allow us to use common filtering by regex before
    # gathering stats.  However, there are still pitfalls.  You may still
    # wind up deleting more of one kind of index than another if you have
    # multiple kinds.  Also, it still won't work on closed indices, so we
    # must filter them out.
    all_indices = get_indices(client, prefix=prefix, suffix=suffix, exclude_pattern=exclude_pattern)
    not_closed = [i for i in all_indices if not index_closed(client, i)]
    csv_indices = ','.join(not_closed)

    stats = client.indices.status(index=csv_indices)
    
    sorted_indices = sorted(
        (
            (index_name, index_stats['index']['primary_size_in_bytes'])
            for (index_name, index_stats) in stats['indices'].items()
        ),
        reverse=True
    )

    for index_name, index_size in sorted_indices:
        disk_usage += index_size

        if disk_usage > disk_limit:
            yield index_name
        else:
            logger.info('skipping {0}, summed disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))

# Operations
## Single-index operations
### Alias
#### Add to alias
def add_to_alias(client, index_name, alias=None, **kwargs):
    """
    Add indicated index to the specified alias.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg alias: Alias name to operate on.
    :rtype: bool
    """
    if not alias: # This prevents _all from being aliased by accident...
        logger.error('No alias provided.')
        return True
    if not client.indices.exists_alias(alias):
        logger.error('Skipping index {0}: Alias {1} does not exist.'.format(index_name, alias))
        return True
    else:
        indices_in_alias = client.indices.get_alias(alias)
        if not index_name in indices_in_alias:
            client.indices.update_aliases(body={'actions': [{ 'add': { 'index': index_name, 'alias': alias}}]})
        else:
            logger.info('Skipping index {0}: Index already exists in alias {1}...'.format(index_name, alias))
            return True

#### Remove from alias
def remove_from_alias(client, index_name, alias=None, **kwargs):
    """
    Remove the indicated index from the specified alias.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg alias: Alias name to operate on.
    :rtype: bool
    """
    indices_in_alias = get_alias(client, alias)
    if not indices_in_alias:
        return True
    if index_name in indices_in_alias:
        client.indices.update_aliases(body={'actions': [{ 'remove': { 'index': index_name, 'alias': alias}}]})
    else:
        logger.info('Index {0} does not exist in alias {1}; skipping.'.format(index_name, alias))
        return True

### Allocation
def apply_allocation_rule(client, index_name, rule=None, **kwargs):
    """
    Apply a required allocation rule to an index.  See:
    http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index-modules-allocation.html#index-modules-allocation

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg rule: The routing allocation rule to apply, e.g. ``tag=ssd``.  Must be
        in the format of ``key=value``, and should match values declared on the
        correlating nodes in your cluster.
    """
    if not rule:
        logger.error('No rule provided for {0}.'.format(index_name))
        return True
    key = rule.split('=')[0]
    value = rule.split('=')[1]
    if index_closed(client, index_name):
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        logger.info('Updating index setting index.routing.allocation.require.{0}={1}'.format(key,value))
        client.indices.put_settings(index=index_name, body='index.routing.allocation.require.{0}={1}'.format(key,value))

### Bloom
def disable_bloom_filter(client, index_name, **kwargs):
    """
    Disable the bloom filter cache for the specified index.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    """
    if index_closed(client, index_name): # Don't try to disable bloom filter on a closed index.  It will re-open them
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.put_settings(index=index_name, body='index.codec.bloom.load=false')

### Change Replica Count
def change_replicas(client, index_name, replicas=None, **kwargs):
    """
    Change the number of replicas, more or less, for the indicated index.
    
    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg replicas: The number of replicas the index should have
    """
    if replicas == None:
        logger.error('No replica count provided for {0}.'.format(index_name))
        return True
    if index_closed(client, index_name):
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        logger.debug('Previous count for number_of_replicas={0}'.format(client.indices.get_settings(
            index=index_name)[index_name]['settings']['index']['number_of_replicas']))
        logger.info('Updating index setting number_of_replicas={0}'.format(replicas))
        client.indices.put_settings(index=index_name, body='number_of_replicas={0}'.format(replicas))

### Close
def close_index(client, index_name, **kwargs):
    """
    Close the indicated index.  Flush before closing.
    
    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    """
    if index_closed(client, index_name):
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.flush(index=index_name)
        client.indices.close(index=index_name)

### Delete
def delete_index(client, index_name, **kwargs):
    """
    Delete the indicated index.
    
    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    """
    client.indices.delete(index=index_name)

### Optimize
def optimize_index(client, index_name, max_num_segments=2, **kwargs):
    """
    Optimize (Lucene forceMerge) index to ``max_num_segments`` per shard
    
    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg max_num_segments: Merge to this number of segments per shard.
    """
    if index_closed(client, index_name): # Don't try to optimize a closed index
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        shards, segmentcount = get_segmentcount(client, index_name)
        logger.debug('Index {0} has {1} shards and {2} segments total.'.format(index_name, shards, segmentcount))
        if segmentcount > (shards * max_num_segments):
            logger.info('Optimizing index {0} to {1} segments per shard.  Please wait...'.format(index_name, max_num_segments))
            client.indices.optimize(index=index_name, max_num_segments=max_num_segments)
        else:
            logger.info('Skipping index {0}: Already optimized.'.format(index_name))
            return True

## Snapshots
### Create a snapshot body
def create_snapshot_body(indices, ignore_unavailable=False,
                         include_global_state=True, partial=False, **kwargs):
    """
    Create the request body for creating a snapshot from the provided
    arguments.

    :arg indices: A single index, or list of indices to snapshot.
    :arg ignore_unavailable: Boolean. Ignore unavailable shards/indices.
        Default is `False`
    :arg include_global_state: Boolean. Store cluster global state with snapshot.
        Default is `True`
    :arg partial: Boolean. Do not fail if primary shard is unavailable. Default
        is `False`
    """
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if indices == '_all':
        body["indices"] = indices
    else:
        if type(indices) is not type(list()):   # in case of a single value passed
            indices = [indices]
        body["indices"] = ','.join(sorted(indices))
    return body

### Create a snapshot
def create_snapshot(client, indices='_all', snapshot_name=None,
                    snapshot_prefix='curator-', repository='',
                    ignore_unavailable=False, include_global_state=True,
                    partial=False, wait_for_completion=True, **kwargs):
    """
    Create a snapshot of provided indices (or ``_all``) that are open.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to snapshot. Default is ``_all``
    :arg snapshot_name: What to name the snapshot. ``snapshot_prefix`` +
        datestamp if omitted.
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    :arg repository: The Elasticsearch snapshot repository to use
    :arg wait_for_completion: Wait (or not) for the operation
        to complete before returning.  Waits by default, i.e. Default is
        `True`
    :type wait_for_completion: bool
    :arg ignore_unavailable: Ignore unavailable shards/indices.
        Default is `False`
    :type ignore_unavailable: bool
    :arg include_global_state: Store cluster global state with snapshot.
        Default is `True`
    :type include_global_state: bool
    :arg partial: Do not fail if primary shard is unavailable. Default
        is `False`
    :type partial: bool
    """
    # Return True if it is skipped
    if not repository:
        logger.error("Unable to create snapshot. Repository name not provided.")
        return True
    try:
        if not indices == '_all':
            if type(indices) == type(list()):
                indices = [i for i in indices if not index_closed(client, i)]
            else:
                indices = indices if not index_closed(client, indices) else ''
        body=create_snapshot_body(indices, ignore_unavailable=ignore_unavailable, include_global_state=include_global_state, partial=partial)
        datestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        snapshot_name = snapshot_name if snapshot_name else snapshot_prefix + datestamp
        logger.info("Snapshot name: {0}".format(snapshot_name))
        all_snaps = get_snaplist(client, repository=repository, snapshot_prefix=snapshot_prefix)
        if not snapshot_name in all_snaps and len(indices) > 0:
            client.snapshot.create(repository=repository, snapshot=snapshot_name, body=body, wait_for_completion=wait_for_completion)
        elif len(indices) == 0:
            logger.warn("No indices provided.")
            return True
        else:
            logger.info("Skipping: A snapshot with name '{0}' already exists.".format(snapshot_name))
            return True
    except elasticsearch.RequestError as e:
        logger.error("Unable to create snapshot {0}.  Error: {1} Check logs for more information.".format(snapshot_name, e))
        return True

### Delete a snapshot
def delete_snapshot(client, snap, **kwargs):
    """
    Delete a snapshot (or comma-separated list of snapshots)

    :arg client: The Elasticsearch client connection
    :arg snap: The snapshot name
    :arg repository: The Elasticsearch snapshot repository to use
    """
    if not "repository" in kwargs:
        logger.error("Repository information omitted. Must specify repository to delete snapshot.")
    else:
        repository = kwargs["repository"]
    try:
        client.snapshot.delete(repository=repository, snapshot=snap)
    except elasticsearch.RequestError as e:
        logger.error("Unable to delete snapshot {0}.  Error: {1} Check logs for more information.".format(snap, e))

# Operations typically used by the curator_script, directly or indirectly
## Loop through a list of objects and perform the indicated operation
def _op_loop(client, object_list, op=None, dry_run=False, **kwargs):
    """
    Perform the ``op`` on indices or snapshots in the ``object_list``
    
    .. note::
       All kwargs are passed on to the specified ``op``.
       
       Any arg the ``op`` needs *must* be passed in **kwargs**.
    
    :arg client: The Elasticsearch client connection
    :arg object_list: The list of indices or snapshots
    :arg op: The operation to perform on each object in ``object_list``
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg delay: Can be specified to pause after each iteration in the loop.
        Useful to allow cluster to quiesce after heavy I/O optimizations.
    """
    if not op:
        logger.error("No operation specified.")
        return
    prepend = kwargs['prepend'] if 'prepend' in kwargs else ''
    for item in object_list:
        if dry_run: # Don't act on a dry run
            logger.info(prepend + "of {0} operation on {1}".format(op.__name__, item))
            continue
        skipped = op(client, item, **kwargs)
        if skipped:
            continue
        if 'delay' in kwargs:
            if kwargs['delay'] > 0:
                logger.info('Pausing for {0} seconds to allow cluster to quiesce...'.format(kwargs['delay']))
                time.sleep(kwargs['delay'])
        logger.info("{0} operation succeeded on {1}".format(op.__name__, item))

## curator alias [ARGS]
def alias(client, dry_run=False, **kwargs):
    """
    Multiple use cases:
    
    1. Add indices ``alias_older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix`` to ``alias``.
    2. Remove indices ``unalias_older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix`` to ``alias``.

    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:
    
       :py:func:`curator.curator.get_object_list`
    
       :py:func:`curator.curator.filter_by_timestamp`
    
       :py:func:`curator.curator.add_to_alias`

       :py:func:`curator.curator.add_to_alias`
    
    These defaults are included here for documentation.
    
    :arg client: The Elasticsearch client connection
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg alias: Alias name to operate on.
    :arg alias_older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg unalias_older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg prefix: A string that comes before the datestamp in an index name. Can
        be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name. Can
        be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg exclude_pattern: Exclude indices matching the provided regular expression.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    logging.info(kwargs['prepend'] + "Performing alias operations...")
    if kwargs['alias_older_than']:
        kwargs['older_than'] = kwargs['alias_older_than']
        op = add_to_alias
    elif kwargs['unalias_older_than']:
        kwargs['older_than'] = kwargs['unalias_older_than']
        op = remove_from_alias
    else:
        op = None
    index_list = get_object_list(client, **kwargs)
    matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
    _op_loop(client, matching_indices, op=op, dry_run=dry_run, **kwargs)
    logger.info(kwargs['prepend'] + 'Alias operations completed for specified indices.')

## curator allocation [ARGS]
def allocation(client, dry_run=False, **kwargs):
    """
    Change shard/routing allocation for indices ``older_than`` *n*
    ``time_unit``\s, matching the given ``timestring``, ``prefix``, and
    ``suffix`` by updating/applying the provided ``rule``.
    
    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:
    
       :py:func:`curator.curator.get_object_list`
    
       :py:func:`curator.curator.filter_by_timestamp`
    
       :py:func:`curator.curator.apply_allocation_rule`
    
       These defaults are included here for documentation.

    :arg client: The Elasticsearch client connection
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg rule: The routing allocation rule to apply, e.g. ``tag=ssd``.  Must be
        in the format of ``key=value``, and should match values declared on the
        correlating nodes in your cluster.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    logging.info(kwargs['prepend'] + "Applying allocation/routing tags to indices...")
    index_list = get_object_list(client, **kwargs)
    matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
    _op_loop(client, matching_indices, op=apply_allocation_rule, dry_run=dry_run, **kwargs)
    logger.info(kwargs['prepend'] + 'Allocation/routing tags applied to specified indices.')

## curator bloom [ARGS]
def bloom(client, dry_run=False, **kwargs):
    """
    Disable bloom filter cache for indices ``older_than`` *n* ``time_unit``\s,
    matching the given ``timestring``, ``prefix``, and ``suffix``.

    Can optionally ``delay`` a given number of seconds after each optimization.

    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:
    
       :py:func:`curator.curator.get_object_list`
    
       :py:func:`curator.curator.filter_by_timestamp`
    
       :py:func:`curator.curator.disable_bloom_filter`
    
       These defaults are included here for documentation.

    :arg client: The Elasticsearch client connection
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :arg delay: Pause *n* seconds after optimizing an index.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    logging.info(kwargs['prepend'] + "Disabling the bloom filter cache for indices...")
    index_list = get_object_list(client, **kwargs)
    matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
    _op_loop(client, matching_indices, op=disable_bloom_filter, dry_run=dry_run, **kwargs)
    logger.info(kwargs['prepend'] + 'Disabled bloom filter cache for specified indices.')

## curator close [ARGS]
def close(client, dry_run=False, **kwargs):
    """
    Close indices ``older_than`` *n* ``time_unit``\s, matching the given
    ``timestring``, ``prefix``, and ``suffix``.
    
    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:
    
       :py:func:`curator.curator.get_object_list`
    
       :py:func:`curator.curator.filter_by_timestamp`
    
       :py:func:`curator.curator.close_index`
    
       These defaults are included here for documentation.

    :arg client: The Elasticsearch client connection
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    logging.info(kwargs['prepend'] + "Closing indices...")
    index_list = get_object_list(client, **kwargs)
    matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
    _op_loop(client, matching_indices, op=close_index, dry_run=dry_run, **kwargs)
    logger.info(kwargs['prepend'] + 'Closed specified indices.')

## curator delete [ARGS]
def delete(client, dry_run=False, **kwargs):
    """
    Two use cases for deleting indices:
    
    1. Delete indices ``older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix``.
    2. Delete indices in excess of ``disk_space`` gigabytes if the ``disk_space`` kwarg is present, beginning from the oldest.  Indices must still match ``prefix`` and ``suffix``.  This amount spans all nodes and shards and must be calculated accordingly.  This is not a recommended use-case.

    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:

       :py:func:`curator.curator.get_object_list`

       :py:func:`curator.curator.filter_by_space`

       :py:func:`curator.curator.filter_by_timestamp`

       :py:func:`curator.curator.delete_index`

       These defaults are included here for documentation.

    :arg client: The Elasticsearch client connection
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg disk_space: Delete indices over *n* gigabytes, starting from the
        oldest indices.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    logging.info(kwargs['prepend'] + "Deleting indices...")
    by_space = kwargs['disk_space'] if 'disk_space' in kwargs else False
    if by_space:
        logger.info(kwargs['prepend'] + 'Deleting by space rather than time.')
        matching_indices = filter_by_space(client, **kwargs)
    else:
        index_list = get_object_list(client, **kwargs)
        matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
    _op_loop(client, matching_indices, op=delete_index, dry_run=dry_run, **kwargs)
    logger.info(kwargs['prepend'] + 'Specified indices deleted.')

## curator optimize [ARGS]
def optimize(client, dry_run=False, **kwargs):
    """
    Optimize indices ``older_than`` *n* ``time_unit``\s, matching the given
    ``timestring``, ``prefix``, and ``suffix`` to ``max_num_segments`` segments
    per shard.
    
    Can optionally ``delay`` a given number of seconds after each optimization.
    
    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:
       
       :py:func:`curator.curator.get_object_list`
       
       :py:func:`curator.curator.filter_by_timestamp`
       
       :py:func:`curator.curator.optimize_index`

       These defaults are included here for documentation.

    :arg client: The Elasticsearch client connection
    :arg dry_run: If True, simulate, but do not perform the operation
    :arg older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg max_num_segments: Merge to this number of segments per shard.
    :arg delay: Pause *n* seconds after optimizing an index.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    logging.info(kwargs['prepend'] + "Optimizing indices...")
    index_list = get_object_list(client, **kwargs)
    matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
    _op_loop(client, matching_indices, op=optimize_index, dry_run=dry_run, **kwargs)
    logger.info(kwargs['prepend'] + 'Optimized specified indices.')

## curator (change) replicas [ARGS]
def replicas(client, dry_run=False, **kwargs):
    """
    Change replica count for indices ``older_than`` *n* ``time_unit``\s, 
    matching the given ``timestring``, ``prefix``, and ``suffix``.
    
    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:
    
       :py:func:`curator.curator.get_object_list`
    
       :py:func:`curator.curator.filter_by_timestamp`
    
       :py:func:`curator.curator.close_index`
    
       These defaults are included here for documentation.

    :arg client: The Elasticsearch client connection
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg replicas: The number of replicas the index should have
    :arg older_than: Indices older than the indicated number of whole
        ``time_units`` will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    logging.info(kwargs['prepend'] + "Changing replica count of indices...")
    index_list = get_object_list(client, **kwargs)
    matching_indices = filter_by_timestamp(object_list=index_list, **kwargs)
    _op_loop(client, matching_indices, op=change_replicas, dry_run=dry_run, **kwargs)
    logger.info(kwargs['prepend'] + 'Changing replica count for specified indices.')

## curator snapshot [ARGS]
def snapshot(client, dry_run=False, **kwargs):
    """
    Multiple use cases:
    
    1. Snapshot indices ``older_than`` *n* ``time_unit``\s, matching the given ``timestring``, ``prefix``, and ``suffix`` into ``repository``.
    2. Snapshot *n* ``most_recent`` indices matching ``prefix`` and ``suffix`` into ``repository``.
    3. ``delete_older_than`` *n* ``time_units`` matching the given ``timestring``, ``prefix``, and ``suffix`` from ``repository``.
    4. Snapshot ``all_indices`` (ignoring ``older_than`` and ``most_recent``).

    .. note::
       As this is an iterative function, default values are handled by the
       target function(s).
       
       Unless passed in `kwargs`, parameters other than ``client`` and
       ``dry_run`` will have default values assigned by the functions being
       called:
       
       :py:func:`curator.curator.get_object_list`
       
       :py:func:`curator.curator.filter_by_timestamp`
       
       :py:func:`curator.curator.create_snapshot`
       
       :py:func:`curator.curator.delete_snapshot`
       
       These defaults are included here for documentation.

    :arg client: The Elasticsearch client connection
    :arg dry_run: If true, simulate, but do not perform the operation
    :arg older_than: Indices older than the indicated number of whole
        ``time_unit``\s will be operated on.
    :arg most_recent: Most recent *n* indices will be operated on.
    :arg all_indices: Boolean.  Include ``_all`` indices in snapshot.
    :type all_indices: bool
    :arg delete_older_than: Snapshots older than the indicated number of whole
        ``time_unit``\s will be operated on.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg snapshot_name: Override the default with this value. Defaults to `None`
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    :arg prefix: A string that comes before the datestamp in an index name.
        Can be empty. Wildcards acceptable.  Default is ``logstash-``.
    :arg suffix: A string that comes after the datestamp of an index name.
        Can be empty. Wildcards acceptable.  Default is empty, ``''``.
    :arg repository: The Elasticsearch snapshot repository to use
    :arg wait_for_completion: Wait (or not) for the operation
        to complete before returning.  Waits by default, i.e. Default is
        `True`
    :type wait_for_completion: bool
    :arg ignore_unavailable: Ignore unavailable shards/indices.
        Default is `False`
    :type ignore_unavailable: bool
    :arg include_global_state: Store cluster global state with snapshot.
        Default is `True`
    :type include_global_state: bool
    :arg partial: Do not fail if primary shard is unavailable. Default
        is `False`
    :type partial: bool
    :arg exclude_pattern: Exclude indices matching the provided regular
        expression.
    :arg utc_now: Used for testing.  Overrides current time with specified time.
    """
    if not 'repository' in kwargs:
        logger.error("Repository name not provided.")
        return
    kwargs['prepend'] = "DRY RUN: " if dry_run else ''
    if not 'older_than' in kwargs and not 'most_recent' in kwargs and not 'delete_older_than' in kwargs and not 'all_indices'in kwargs:
        logger.error('Expect missing argument.')
        return
    # Preserving kwargs intact for passing to _op_loop is the game here...
    all_indices       = kwargs['all_indices'] if 'all_indices' in kwargs else False
    delete_older_than = kwargs['delete_older_than'] if 'delete_older_than' in kwargs else False
    older_than        = kwargs['older_than'] if 'older_than' in kwargs else False
    most_recent       = kwargs['most_recent'] if 'most_recent' in kwargs else False
    if delete_older_than:
        logger.info(kwargs['prepend'] + "Deleting specified snapshots...")
        kwargs['older_than'] = kwargs['delete_older_than'] # Fix for delete in this case only.
        snapshot_list = client.snapshot.get(repository=kwargs['repository'], snapshot="_all")['snapshots']
        matching_snapshots = list(filter_by_timestamp(object_list=snapshot_list, object_type='snapshot', **kwargs))
        _op_loop(client, matching_snapshots, op=delete_snapshot, dry_run=dry_run, **kwargs)
        logger.info(kwargs['prepend'] + 'Specified snapshots deleted.')
    else:
        logger.info(kwargs['prepend'] + "Capturing snapshots of specified indices...")
        if not all_indices:
            index_list = get_object_list(client, **kwargs)
            if most_recent:
                matching_indices = index_list[-kwargs['most_recent']:]
            elif older_than:
                matching_indices = list(filter_by_timestamp(object_list=index_list, **kwargs))
            else:
                logger.error(kwargs['prepend'] + 'Missing argument: Must provide one of: older_than, most_recent, all_indices, delete_older_than')
                return
        else:
            matching_indices = '_all'
        logger.info(kwargs['prepend'] + 'Snapshot will capture indices: {0}'.format(', '.join(matching_indices)))
        if not dry_run:
            # Default `create_snapshot` behavior is to snap `_all` into a
            # snapshot named `snapshot_name` or `curator-%Y-%m-%dT%H:%M:%S`
            create_snapshot(client, indices=matching_indices, **kwargs)
        logger.info(kwargs['prepend'] + 'Snapshots captured for specified indices.')
