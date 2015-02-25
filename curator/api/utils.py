import elasticsearch
import time
import re
from datetime import timedelta, datetime, date

import logging
logger = logging.getLogger(__name__)

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
    elif len(indices) == 1:
        return indices[0]
    else:
        return None

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
    :rtype: list
    """
    indices = ensure_list(indices)
    if '.marvel-kibana' in indices:
        indices.remove('.marvel-kibana')
    if 'kibana-int' in indices:
        indices.remove('kibana-int')
    if '.kibana' in indices:
        indices.remove('.kibana')
    return indices

### Index state
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

## Client state
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
