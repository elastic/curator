#!/usr/bin/env python

import os
import logging
import re
from datetime import timedelta, datetime, date

import elasticsearch

__version__ = '2.0.0-dev'

logger = logging.getLogger(__name__)

DATE_REGEX = {
    'Y' : '4',
    'm' : '2',
    'W' : '2',
    'U' : '2',
    'd' : '2',
    'H' : '2',
}

def get_date_regex(timestring):
    """Turn a supported strftime string into a regex"""
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
    """ Gets the time of the index.

    :param index_timestamp: A string of the format timestring
    :return The creation time (datetime) of the index.
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

def find_target_month(month_count, utc_now=None):
    """Return datetime object for # of _full_ months older than month_count from now"""
    utc_now = date(utc_now.year, utc_now.month, 1) if utc_now else date.today()
    target_date = date(utc_now.year, utc_now.month, 1)

    for i in range(0, month_count):
        if target_date.month == 1:
            target_date = date(target_date.year-1, 12, 1)
        else:
            target_date = date(target_date.year, target_date.month-1, 1)

    return datetime(target_date.year, target_date.month, target_date.day)

def get_indices(client, prefix='logstash-', suffix='', exclude_pattern=None):
    """Return a sorted list of indices matching prefix"""
    _indices = sorted(client.indices.get_settings(index='*', params={'expand_wildcards': 'closed'}).keys())
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
    
def get_snaplist(client, repo_name, prefix='logstash-', suffix=''):
    """Get _all snapshots containing prefix from repo_name and return a list"""
    retval = []
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        retval = [snap['snapshot'] for snap in allsnaps if 'snapshot' in snap.keys()]
        if not prefix == '*':
            retval = [i for i in retval if i.startswith(prefix)]
        if not suffix == '' and not suffix == '*':
            retval = [i for i in retval if i.endswith(suffix)]
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
    return retval

def get_snapped_indices(client, repo_name, prefix='logstash-'):
    """Return all indices in snapshots which succeeded and match prefix"""
    from itertools import chain
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        allindices = chain.from_iterable(s['indices'] for s in allsnaps if s['state'] == 'SUCCESS')
        return set(i for i in allindices if i.startswith(prefix))
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
        return []

def get_version(client):
    """Return ES version number as a tuple"""
    version = client.info()['version']['number']
    return tuple(map(int, version.split('.')))

def is_master_node(client):
    my_node_id = client.nodes.info('_local')['nodes'].keys()[0]
    master_node_id = client.cluster.state(metric='master_node')['master_node']
    return my_node_id == master_node_id

def get_object_list(client, data_type='index', prefix='logstash-', suffix='', repository=None, exclude_pattern=None, **kwargs):
    """Return a list of indices or snapshots"""
    if data_type == 'index':
        object_list = get_indices(client, prefix=prefix, suffix=suffix)
    elif data_type == 'snapshot':
        if repository:
            object_list = get_snaplist(client, repository, prefix=prefix, suffix=suffix)
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
    
def find_expired_data(object_list=[], utc_now=None, time_unit='days', older_than=999999, prefix='logstash-', suffix='', timestring=None, **kwargs):
    """ Generator that yields expired objects (indices or snapshots).
    
    :return: Yields a list of indices older than n `time_unit`s
    """
    if prefix:
        prefix = '.' + prefix if prefix[0] == '*' else prefix
    if suffix:
        suffix = '.' + suffix if suffix[0] == '*' else suffix
    dateregex = get_date_regex(timestring)
    regex = "^" + prefix + "(" + dateregex + ")" + suffix + "$"

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
        cutoff = find_target_month(older_than, utc_now=utc_now)
    else:
        # This cutoff must be a multiple of time_units
        cutoff = utc_now - timedelta(**{time_unit: (older_than - 1)})
    
    for object_name in object_list:
        try:
            index_timestamp = re.search(regex, object_name).group(1)
        except AttributeError as e:
            logger.debug('Unable to match {0} with regular expression {1}.  Error: {2}'.format(object_name, regex, e))
            continue
        # index_timestamp = object_name[len(prefix):]

        try:
            object_time = get_index_time(index_timestamp, timestring)
        except ValueError:
            logger.error('Could not find a valid timestamp for {0} with timestring {1}'.format(object_name, timestring))
            continue
    
        # if the index is older than the cutoff
        if object_time < cutoff:
            yield object_name
        else:
            logger.info('{0} is within the threshold period ({1} {2}).'.format(object_name, older_than, time_unit))

def find_overusage_indices(client, disk_space=2097152.0, prefix='logstash-', **kwargs):
    """ Generator that yields over usage indices.

    :return: Yields a list of indices to delete based on space consumed, starting with the oldest.
    """

    disk_usage = 0.0
    disk_limit = disk_space * 2**30

    stats = client.indices.status(index=prefix+'*')
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
            yield index_name, 0
        else:
            logger.info('skipping {0}, disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))

def index_closed(client, index_name):
    """Return True if index is closed"""
    index_metadata = client.cluster.state(
        index=index_name,
        metric='metadata',
    )
    return index_metadata['metadata']['indices'][index_name]['state'] == 'close'

def create_snapshot_body(indices, ignore_unavailable=False, include_global_state=False, partial=False, **kwargs):
    """Create the request body for creating a snapshot"""
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if type(indices) is not type(list()):   # in case of a single value passed
        indices = [indices]
    body["indices"] = ','.join(sorted(indices))
    return body
    
def get_repository(client, repo_name):
    """Get Repository information"""
    try:
        return client.snapshot.get_repository(repository=repo_name)
    except elasticsearch.NotFoundError as e:
        logger.info("Repository {0} not found.  Error: {1}".format(repo_name, e))
        return None

def get_snapshot(client, repo_name, snap_name):
    """Get information about a snapshot (or snapshots)"""
    try:
        return client.snapshot.get(repository=repo_name, snapshot=snap_name)
    except elasticsearch.NotFoundError as e:
        logger.info("Snapshot or repository {0} not found.  Error: {1}".format(snap_name, e))
        return None

def create_snapshot(client, snap_name, prefix='logstash-', suffix='', repository=None, ignore_unavailable=False, include_global_state=False, partial=False, wait_for_completion=True, **kwargs):
    """Create a snapshot (or snapshots). Overwrite failures"""
    # Return True when it was skipped
    if not repository:
        logger.error("Unable to create snapshot. Repository name not provided.")
        return True
    try:
        successes = get_snapped_indices(client, repository, prefix=prefix)
        snaps = get_snaplist(client, repository, prefix=prefix, suffix=suffix)
        closed = index_closed(client, snap_name)
        body=create_snapshot_body(snap_name, ignore_unavailable=ignore_unavailable, include_global_state=include_global_state, partial=partial)
        if not snap_name in snaps and not snap_name in successes and not closed:
            client.snapshot.create(repository=repository, snapshot=snap_name, body=body, wait_for_completion=wait_for_completion)
        elif snap_name in snaps and not snap_name in successes and not closed:
            logger.warn("Previous snapshot was unsuccessful.  Deleting snapshot {0} and trying again.".format(snap_name))
            delete_snapshot(client, repository, snap_name)
            client.snapshot.create(repository=repository, snapshot=snap_name, body=body, wait_for_completion=wait_for_completion)
        elif closed:
            logger.info("Skipping: Index {0} is closed.".format(snap_name))
            return True
        else:
            logger.info("Skipping: A snapshot with name '{0}' already exists.".format(snap_name))
            return True
    except elasticsearch.RequestError as e:
        logger.error("Unable to create snapshot {0}.  Error: {1} Check logs for more information.".format(snap_name, e))
        return True

def delete_snapshot(client, snap_name, repository=None, **kwargs):
    """Delete a snapshot (or snapshots)"""
    # kwargs is here to preserve expected number of args passed by command_loop
    client.snapshot.delete(repository=repository, snapshot=snap_name)
    
def close_index(client, index_name, **kwargs):
    if index_closed(client, index_name):
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.close(index=index_name)

def delete_index(client, index_name, **kwargs):
    client.indices.delete(index=index_name)

def optimize_index(client, index_name, max_num_segments=2, **kwargs):
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

def bloom_index(client, index_name, **kwargs):
    if index_closed(client, index_name): # Don't try to disable bloom filter on a closed index.  It will re-open them
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.put_settings(index=index_name, body='index.codec.bloom.load=false')
        
def require_index(client, index_name, rule=None, **kwargs):
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

def get_alias(client, alias):
    if client.indices.exists_alias(alias):
        return client.indices.get_alias(name=alias).keys()
    else:
        logger.error('Unable to find alias {0}.'.format(alias))
        return False

def remove_from_alias(client, index_name, alias=None, **kwargs):
    indices_in_alias = get_alias(client, alias)
    if not indices_in_alias:
        return True
    if index_name in indices_in_alias:
        client.indices.update_aliases(body={'actions': [{ 'remove': { 'index': index_name, 'alias': alias}}]})
    else:
        logger.info('Index {0} does not exist in alias {1}; skipping.'.format(index_name, alias))
        return True

def add_to_alias(client, index_name, alias=None, **kwargs):
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
    
def get_segmentcount(client, index_name):
    """Return a list of shardcount, segmentcount"""
    shards = client.indices.segments(index=index_name)['indices'][index_name]['shards']
    segmentcount = 0
    totalshards = 0 # We will increment this manually to capture all replicas...
    for shardnum in shards:
        for shard in range(0,len(shards[shardnum])):
            segmentcount += shards[shardnum][shard]['num_search_segments']
            totalshards += 1
    return totalshards, segmentcount

