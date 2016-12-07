import os
from voluptuous import *

# Elasticsearch versions supported
def version_max():
    return (5, 1, 99)
def version_min():
    return (2, 0, 0)

# Default Config file location
def config_file():
    return os.path.join(os.path.expanduser('~'), '.curator', 'curator.yml')

# Default filter patterns (regular expressions)
def regex_map():
    return {
        'timestring': r'^.*{0}.*$',
        'regex': r'{0}',
        'prefix': r'^{0}.*$',
        'suffix': r'^.*{0}$',
    }

def date_regex():
    return {
        'Y' : '4',
        'y' : '2',
        'm' : '2',
        'W' : '2',
        'U' : '2',
        'd' : '2',
        'H' : '2',
        'M' : '2',
        'S' : '2',
        'j' : '3',
    }

# Actions

def cluster_actions():
    return [ 'cluster_routing' ]

def index_actions():
    return [
        'alias',
        'allocation',
        'close',
        'create_index',
        'delete_indices',
        'forcemerge',
        'open',
        'replicas',
        'snapshot',
    ]

def snapshot_actions():
    return [ 'delete_snapshots', 'restore' ]

def all_actions():
    return sorted(cluster_actions() + index_actions() + snapshot_actions())

def index_filtertypes():
    return [
        'alias',
        'allocated',
        'age',
        'closed',
        'count',
        'forcemerged',
        'kibana',
        'none',
        'opened',
        'pattern',
        'space',
    ]

def snapshot_filtertypes():
    return ['age', 'count', 'none', 'pattern', 'state']

def all_filtertypes():
    return sorted(list(set(index_filtertypes() + snapshot_filtertypes())))

def default_options():
    return {
        'continue_if_exception': False,
        'disable_action': False,
        'ignore_empty_list': False,
        'timeout_override': None,
    }

def default_filters():
    return { 'filters' : [{ 'filtertype' : 'none' }] }
