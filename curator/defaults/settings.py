"""Utilities/Helpers for defaults and schemas"""

from os import path
from voluptuous import Any, Boolean, Coerce, Optional
from curator.exceptions import CuratorException

# pylint: disable=E1120

CURATOR_DOCS = 'https://www.elastic.co/guide/en/elasticsearch/client/curator'
CLICK_DRYRUN = {
    'dry-run': {'help': 'Do not perform any changes.', 'is_flag': True},
}
DATA_NODE_ROLES = ['data', 'data_content', 'data_hot', 'data_warm']

# Click specifics


def footer(version, tail='index.html'):
    """
    Generate a footer linking to Curator docs based on Curator version

    :param version: The Curator version

    :type version: str

    :returns: An epilog/footer suitable for Click
    """
    if not isinstance(version, str):
        raise CuratorException('Parameter version is not a string: {type(version)}')
    majmin = ''
    try:
        ver = version.split('.')
        majmin = f'{ver[0]}.{ver[1]}'
    except Exception as exc:
        msg = f'Could not determine Curator version from provided value: {version}'
        raise CuratorException(msg) from exc
    return f'Learn more at {CURATOR_DOCS}/{majmin}/{tail}'


# Default Config file location
def default_config_file():
    """
    :returns: The default configuration file location:
        path.join(path.expanduser('~'), '.curator', 'curator.yml')
    """
    default = path.join(path.expanduser('~'), '.curator', 'curator.yml')
    if path.isfile(default):
        return default


# Default filter patterns (regular expressions)
def regex_map():
    """
    :returns: A dictionary of pattern filter 'kind's with their associated regular
        expression: {'timestring': r'^.*{0}.*$', 'regex': r'{0}',
        'prefix': r'^{0}.*$', 'suffix': r'^.*{0}$'}
    """
    return {
        'timestring': r'^.*{0}.*$',
        'regex': r'{0}',
        'prefix': r'^{0}.*$',
        'suffix': r'^.*{0}$',
    }


def date_regex():
    """
    :returns: A dictionary/map of the strftime string characters and their string
        lengths: {'Y':'4', 'G':'4', 'y':'2', 'm':'2', 'W':'2', 'V':'2', 'U':'2',
        'd':'2', 'H':'2', 'M':'2', 'S':'2', 'j':'3'}
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
    :returns: A list of supported cluster actions (right now, that's only
        ['cluster_routing'])
    """
    return ['cluster_routing']


def index_actions():
    """
    :returns: The list of supported index actions:
        [ 'alias', 'allocation', 'close', 'create_index', 'delete_indices',
        'forcemerge', 'index_settings', 'open', 'reindex', 'replicas',
        'rollover', 'shrink', 'snapshot']
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
    :returns: The list of supported snapshot actions: ['delete_snapshots', 'restore']
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
        ['alias', 'allocated', 'age', 'closed', 'count', 'empty', 'forcemerged',
        'ilm', 'kibana', 'none', 'opened', 'pattern', 'period', 'space',
        'shards', 'size']
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
    :returns: The list of supported snapshot filter types: ['age', 'count', 'none',
        'pattern', 'period', 'state']
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
        {'allow_ilm_indices': False, 'continue_if_exception': False,
        'disable_action': False, 'ignore_empty_list': False,
        'timeout_override': None}
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

    return {
        Optional('aliases'): Any(list, str),
        Optional('allocation_type'): Any(str),
        Optional('count'): Coerce(int),
        Optional('date_from'): Any(None, str),
        Optional('date_from_format'): Any(None, str),
        Optional('date_to'): Any(None, str),
        Optional('date_to_format'): Any(None, str),
        Optional('direction'): Any(str),
        Optional('disk_space'): float,
        Optional('epoch'): Any(Coerce(int), None),
        Optional('exclude'): Any(None, bool, int, str),
        Optional('field'): Any(None, str),
        Optional('intersect'): Any(None, bool, int, str),
        Optional('key'): Any(str),
        Optional('kind'): Any(str),
        Optional('max_num_segments'): Coerce(int),
        Optional('number_of_shards'): Coerce(int),
        Optional('pattern'): Any(str),
        Optional('period_type'): Any(str),
        Optional('reverse'): Any(None, bool, int, str),
        Optional('range_from'): Coerce(int),
        Optional('range_to'): Coerce(int),
        Optional('shard_filter_behavior'): Any(str),
        Optional('size_behavior'): Any(str),
        Optional('size_threshold'): Any(Coerce(float)),
        Optional('source'): Any(str),
        Optional('state'): Any(str),
        Optional('stats_result'): Any(None, str),
        Optional('timestring'): Any(None, str),
        Optional('threshold_behavior'): Any(str),
        Optional('unit'): Any(str),
        Optional('unit_count'): Coerce(int),
        Optional('unit_count_pattern'): Any(str),
        Optional('use_age'): Boolean(),
        Optional('value'): Any(int, float, bool, str),
        Optional('week_starts_on'): Any(None, str),
    }
