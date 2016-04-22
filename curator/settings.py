import os

# Elasticsearch versions supported
VERSION_MAX  = (5, 1, 0)
VERSION_MIN = (2, 0, 0)

CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.curator', 'curator.yml')

CLIENT_DEFAULTS = {
    'hosts': '127.0.0.1',
    'port': 9200,
    'url_prefix': '',
    'http_auth': None,
    'use_ssl': False,
    'certificate': None,
    'client_cert': None,
    'client_key': None,
    'aws_key': None,
    'aws_secret_key': None,
    'aws_region': None,
    'ssl_no_validate': False,
    'timeout': 30,
    'master_only': False,
}

LOGGING_DEFAULTS = {
    'loglevel': 'INFO',
    'logfile': None,
    'logformat': 'default',
}

REGEX_MAP = {
    'timestring': r'^.*{0}.*$',
    'regex': r'{0}',
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
    'j' : '3',
}

ACTION_DEFAULTS = {
    'alias' : { 'alias' : None },
    'allocation' : {
        'key' : None,
        'value' : None,
        'allocation_type' : 'require',
    },
    'close' : {},
    'delete_indices' : {},
    'delete_snapshots' : {
        'repository' : None,
        'retry_interval' : 120,
        'retry_count' : 3,
    },
    'forcemerge' : {
        'delay' : 0,
        'max_num_segments' : 2,
    },
    'open' : {},
    'replicas' : { 'count' : None },
    'snapshot' : {
        'repository' : None,
        'name' : 'curator-%Y%m%d%H%M%S',
        'ignore_unavailable' : False,
        'include_global_state' : True,
        'partial' : False,
        'wait_for_completion' : True,
        'skip_repo_fs_check' : False,
    },
}

F_ARGS_PATTERN = {'kind':None, 'value':None, 'exclude':False}
F_ARGS_AGE_INDEX = {
    'source':'name', 'direction':None, 'timestring':None, 'unit':None,
    'unit_count':None, 'field':None, 'stats_result':'min_value', 'epoch':None,
}
F_ARGS_AGE_SNAPS = {
    'source':'creation_date', 'direction':None, 'timestring':None,
    'unit':None, 'unit_count':None, 'epoch':None,
}
F_ARGS_SPACE = {
    'disk_space':None, 'reverse':True, 'use_age':False,
    'source':'creation_date', 'timestring':None, 'field':None,
    'stats_result':'min_value',
}
F_ARGS_FORCEMERGED = {'max_num_segments':None}
F_ARGS_ALLOCATED = {'key':None, 'value':None, 'allocation_type':'require'}
