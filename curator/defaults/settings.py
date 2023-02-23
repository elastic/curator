"""Utilities/Helpers for defaults and schemas"""
from os import path
from six import string_types
from voluptuous import Any, Boolean, Coerce, Optional

# Elasticsearch versions supported
def version_max():
    """
    :returns: The maximum Elasticsearch version Curator supports: ``(8, 99, 99)``
    """
    return (8, 99, 99)
def version_min():
    """
    :returns: The minimum Elasticsearch version Curator supports: ``(8, 99, 99)``
    """
    return (8, 0, 0)

# Default Config file location
def config_file():
    """
    :returns: The default configuration file location:
        ``path.join(path.expanduser('~'), '.curator', 'curator.yml')``
    """
    return path.join(path.expanduser('~'), '.curator', 'curator.yml')

# Default filter patterns (regular expressions)
def regex_map():
    """
    :returns: A dictionary of pattern filter 'kind's with their associated regular expression:
        ``{'timestring': r'^.*{0}.*$', 'regex': r'{0}', 'prefix': r'^{0}.*$', 'suffix': r'^.*{0}$'}``
    """
    return {
        'timestring': r'^.*{0}.*$',
        'regex': r'{0}',
        'prefix': r'^{0}.*$',
        'suffix': r'^.*{0}$',
    }

def date_regex():
    """
    :returns: A dictionary/map of the strftime string characters and their string lengths:
        ``{'Y':'4', 'G':'4', 'y':'2', 'm':'2', 'W':'2', 'V':'2', 'U':'2', 'd':'2', 'H':'2', 'M':'2', 'S':'2', 'j':'3'}``
    """
    return {
        'Y': '4',
        'G': '4',
        'y': '2',
        'm': '2',
        'W': '2',
        'V': '2',
        'U': '2',
        'd': '2',
        'H': '2',
        'M': '2',
        'S': '2',
        'j': '3',
    }

# Actions

def cluster_actions():
    """
    :returns: A list of supported cluster actions (right now, that's only ``['cluster_routing']``)
    """
    return ['cluster_routing']

def index_actions():
    """
    :returns: The list of supported index actions:
        ``[ 'alias', 'allocation', 'close', 'create_index', 'delete_indices', 'forcemerge', 'index_settings', 'open', 'reindex', 'replicas', 'rollover', 'shrink', 'snapshot']``
    """
    return [
        'alias',
        'allocation',
        'close',
        'cold2frozen',
        'create_index',
        'delete_indices',
        'forcemerge',
        'index_settings',
        'open',
        'reindex',
        'replicas',
        'rollover',
        'shrink',
        'snapshot',
    ]

def snapshot_actions():
    """
    :returns: The list of supported snapshot actions: ``['delete_snapshots', 'restore']``
    """
    return ['delete_snapshots', 'restore']

def all_actions():
    """
    :returns: A sorted list of all supported actions: cluster, index, and snapshot
    """
    return sorted(cluster_actions() + index_actions() + snapshot_actions())

def index_filtertypes():
    """
    :returns: The list of supported index filter types:
        ``['alias', 'allocated', 'age', 'closed', 'count', 'empty', 'forcemerged', 'ilm', 'kibana', 'none', 'opened', 'pattern', 'period', 'space', 'shards', 'size']``
    """

    return [
        'alias',
        'allocated',
        'age',
        'closed',
        'count',
        'empty',
        'forcemerged',
        'ilm',
        'kibana',
        'none',
        'opened',
        'pattern',
        'period',
        'space',
        'shards',
        'size',
    ]

def snapshot_filtertypes():
    """
    :returns: The list of supported snapshot filter types: ``['age', 'count', 'none', 'pattern', 'period', 'state']``
    """
    return ['age', 'count', 'none', 'pattern', 'period', 'state']

def all_filtertypes():
    """
    :returns: A sorted list of all supported filter types (both snapshot and index)
    """
    return sorted(list(set(index_filtertypes() + snapshot_filtertypes())))

def default_options():
    """
    :returns: The default values for these options:
        ``{'allow_ilm_indices': False, 'continue_if_exception': False, 'disable_action': False, 'ignore_empty_list': False, 'timeout_override': None}``
    """
    return {
        'allow_ilm_indices': False,
        'continue_if_exception': False,
        'disable_action': False,
        'ignore_empty_list': False,
        'timeout_override': None,
    }

def default_filters():
    """
    If no filters are set, add a 'none' filter

    :returns: {'filters': [{'filtertype': 'none'}]}
    """
    return {'filters': [{'filtertype': 'none'}]}

def structural_filter_elements():
    """
    :returns: Barebones schemas for initial validation of filters
    """
    # pylint: disable=E1120
    return {
        Optional('aliases'): Any(list, *string_types),
        Optional('allocation_type'): Any(*string_types),
        Optional('count'): Coerce(int),
        Optional('date_from'): Any(None, *string_types),
        Optional('date_from_format'): Any(None, *string_types),
        Optional('date_to'): Any(None, *string_types),
        Optional('date_to_format'): Any(None, *string_types),
        Optional('direction'): Any(*string_types),
        Optional('disk_space'): float,
        Optional('epoch'): Any(Coerce(int), None),
        Optional('exclude'): Any(None, bool, int, *string_types),
        Optional('field'): Any(None, *string_types),
        Optional('intersect'): Any(None, bool, int, *string_types),
        Optional('key'): Any(*string_types),
        Optional('kind'): Any(*string_types),
        Optional('max_num_segments'): Coerce(int),
        Optional('number_of_shards'): Coerce(int),
        Optional('pattern'): Any(*string_types),
        Optional('period_type'): Any(*string_types),
        Optional('reverse'): Any(None, bool, int, *string_types),
        Optional('range_from'): Coerce(int),
        Optional('range_to'): Coerce(int),
        Optional('shard_filter_behavior'): Any(*string_types),
        Optional('size_behavior'): Any(*string_types),
        Optional('size_threshold'): Any(Coerce(float)),
        Optional('source'): Any(*string_types),
        Optional('state'): Any(*string_types),
        Optional('stats_result'): Any(None, *string_types),
        Optional('timestring'): Any(None, *string_types),
        Optional('threshold_behavior'): Any(*string_types),
        Optional('unit'): Any(*string_types),
        Optional('unit_count'): Coerce(int),
        Optional('unit_count_pattern'): Any(*string_types),
        Optional('use_age'): Boolean(),
        Optional('value'): Any(int, float, bool, *string_types),
        Optional('week_starts_on'): Any(None, *string_types),
    }
