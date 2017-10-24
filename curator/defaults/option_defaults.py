from voluptuous import *

# Action Options

def allocation_type():
    return { Optional('allocation_type', default='require'): All(
        Any(str, unicode), Any('require', 'include', 'exclude')) }

def conditions():
    return {
        Optional('conditions'): {
            Optional('max_age'): Any(str, unicode),
            Optional('max_docs'): Coerce(int)
        }
    }

def continue_if_exception():
    return { Optional('continue_if_exception', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def count():
    return { Required('count'): All(Coerce(int), Range(min=0, max=10)) }

def delay():
    return {
        Optional('delay', default=0): All(
                Coerce(float), Range(min=0.0, max=3600.0)
            )
    }

def copy_aliases():
    return { Optional('copy_aliases', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def delete_after():
    return { Optional('delete_after', default=True): Any(bool, All(Any(str, unicode), Boolean())) }

def delete_aliases():
    return { Optional('delete_aliases', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def disable_action():
    return { Optional('disable_action', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def extra_settings():
    return { Optional('extra_settings', default={}): dict }

def ignore_empty_list():
    return { Optional('ignore_empty_list', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def ignore_unavailable():
    return { Optional('ignore_unavailable', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def include_aliases():
    return { Optional('include_aliases', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def include_global_state(action):
    default = False
    if action == 'snapshot':
        default = True
    return { Optional('include_global_state', default=default): Any(bool, All(Any(str, unicode), Boolean())) }

def index_settings():
    return { Required('index_settings'): {'index': dict} }

def indices():
    return { Optional('indices', default=None): Any(None, list) }

def key():
    return { Required('key'): Any(str, unicode) }

def max_num_segments():
    return {
        Required('max_num_segments'): All(Coerce(int), Range(min=1, max=32768))
    }

def max_wait(action):
    # The separation is here in case I want to change defaults later...
    value = -1
    # if action in ['allocation', 'cluster_routing', 'replicas']:
    #     value = -1
    # elif action in ['restore', 'snapshot', 'reindex', 'shrink']:
    #     value = -1
    return { Optional('max_wait', default=value): Any(-1, Coerce(int), None) }

def migration_prefix():
    return { Optional('migration_prefix', default=''): Any(str, unicode, None)}

def migration_suffix():
    return { Optional('migration_suffix', default=''): Any(str, unicode, None)}

def name(action):
    if action in ['alias', 'create_index', 'rollover']:
        return { Required('name'): Any(str, unicode) }
    elif action == 'snapshot':
        return {
            Optional('name', default='curator-%Y%m%d%H%M%S'): Any(str, unicode)
        }
    elif action == 'restore':
        return { Optional('name'): Any(str, unicode) }

def new_index():
    return { Optional('new_index', default=None): Any(str, unicode) }

def node_filters():
    return { 
        Optional('node_filters', default={}): {
          Optional('permit_masters', default=False): Any(bool, All(Any(str, unicode), Boolean())),
          Optional('exclude_nodes', default=[]): Any(list, None)
        }
    }

def number_of_replicas():
    return { Optional('number_of_replicas', default=1): All(Coerce(int), Range(min=0, max=10)) }

def number_of_shards():
    return { Optional('number_of_shards', default=1): All(Coerce(int), Range(min=1, max=99)) }

def partial():
    return { Optional('partial', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def post_allocation():
    return { 
        Optional('post_allocation', default={}): {
          Required('allocation_type', default='require'): All(Any(str, unicode), Any('require', 'include', 'exclude')),
          Required('key'): Any(str, unicode),
          Required('value', default=None): Any(str, unicode, None)
        }
    }

def preserve_existing():
    return { Optional('preserve_existing', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def refresh():
    return { Optional('refresh', default=True): Any(bool, All(Any(str, unicode), Boolean())) }

def remote_aws_key():
    return { Optional('remote_aws_key', default=None): Any(str, unicode, None) }

def remote_aws_secret_key():
    return { Optional('remote_aws_secret_key', default=None): Any(str, unicode, None) }

def remote_aws_region():
    return { Optional('remote_aws_region', default=None): Any(str, unicode, None) }

def remote_certificate():
    return { Optional('remote_certificate', default=None): Any(str, unicode, None) }

def remote_client_cert():
    return { Optional('remote_client_cert', default=None): Any(str, unicode, None) }

def remote_client_key():
    return { Optional('remote_client_key', default=None): Any(str, unicode, None) }

def remote_filters():
    # This is really just a basic check here.  The real check is in the
    # validate_actions() method in utils.py
    return { Optional('remote_filters', default=[
                {
                  'filtertype': 'pattern',
                  'kind': 'regex',
                  'value': '.*',
                  'exclude': True,
                }
            ]
        ): Any(list, None)
    }

def remote_ssl_no_validate():
    return { Optional('remote_ssl_no_validate', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def remote_url_prefix():
    return { Optional('remote_url_prefix', default=''): Any(None, str, unicode) }

def rename_pattern():
    return { Optional('rename_pattern'): Any(str, unicode) }

def rename_replacement():
    return { Optional('rename_replacement'): Any(str, unicode) }

def repository():
    return { Required('repository'): Any(str, unicode) }

def request_body():
    return {
        Required('request_body'): {
            Optional('conflicts'): Any(str, unicode),
            Optional('size'): Coerce(int),
            Required('source'): {
                Required('index'): Any(Any(str, unicode), list),
                Optional('remote'): {
                    Optional('host'): Any(str, unicode),
                    Optional('headers'): Any(str, unicode),
                    Optional('username'): Any(str, unicode),
                    Optional('password'): Any(str, unicode),
                    Optional('socket_timeout'): Any(str, unicode),
                    Optional('connect_timeout'): Any(str, unicode),
                },
                Optional('size'): Coerce(int),
                Optional('type'): Any(Any(str, unicode), list),
                Optional('query'): dict,
                Optional('sort'): dict,
                Optional('_source'): Any(Any(str, unicode), list),
            },
            Required('dest'): {
                Required('index'): Any(str, unicode),
                Optional('type'): Any(Any(str, unicode), list),
                Optional('op_type'): Any(str, unicode),
                Optional('version_type'): Any(str, unicode),
                Optional('routing'): Any(str, unicode),
                Optional('pipeline'): Any(str, unicode),
            },
            Optional('script'): dict,
        }
    }

def requests_per_second():
    return { Optional('requests_per_second', default=-1): Any(
            -1, Coerce(int), None)
    }

def retry_count():
    return {
        Optional('retry_count', default=3): All(
                Coerce(int), Range(min=0, max=100)
            )
    }

def retry_interval():
    return {
        Optional('retry_interval', default=120): All(
                Coerce(int), Range(min=1, max=600)
            )
    }

def routing_type():
    return { Required('routing_type'): Any('allocation', 'rebalance') }

def cluster_routing_setting():
    return { Required('setting'): Any('enable') }

def cluster_routing_value():
    return {
        Required('value'): Any(
                'all', 'primaries', 'none', 'new_primaries', 'replicas'
            )
    }

def shrink_node():
    return { Required('shrink_node'): Any(str, unicode) }

def shrink_prefix():
    return { Optional('shrink_prefix', default=''): Any(str, unicode, None) }

def shrink_suffix():
    return { Optional('shrink_suffix', default='-shrink'): Any(str, unicode, None) }

def skip_repo_fs_check():
    return { Optional('skip_repo_fs_check', default=False): Any(bool, All(Any(str, unicode), Boolean())) }

def slices():
    return { Optional('slices', default=1): Any(
        All(Coerce(int), Range(min=1, max=500)), None)
    }

def timeout(action):
    # if action == 'reindex':
    value = 60
    return { Optional('timeout', default=value): Any(Coerce(int), None) }

def timeout_override(action):
    if action in ['forcemerge', 'restore', 'snapshot']:
        value = 21600
    elif action == 'close':
        value = 180
    else:
        value = None

    return {
        Optional('timeout_override', default=value): Any(Coerce(int), None)
    }

def value():
    return { Required('value', default=None): Any(str, unicode, None) }

def wait_for_active_shards(action):
    value = 0
    if action in ['reindex', 'shrink']:
        value = 1
    return {
        Optional('wait_for_active_shards', default=value): Any(
            Coerce(int), 'all', None)
    }

def wait_for_completion(action):
    # if action in ['reindex', 'restore', 'snapshot']:
    value = True
    if action in ['allocation', 'cluster_routing', 'replicas']:
        value = False
    return { Optional('wait_for_completion', default=value): Any(bool, All(Any(str, unicode), Boolean())) }

def wait_interval(action):
    minval = 1
    maxval = 30
    # if action in ['allocation', 'cluster_routing', 'replicas']:
    value = 3
    if action in ['restore', 'snapshot', 'reindex', 'shrink']:
        value = 9
    return { Optional('wait_interval', default=value): Any(All(
                Coerce(int), Range(min=minval, max=maxval)), None) }

def warn_if_no_indices():
    return { Optional('warn_if_no_indices', default=False): Any(bool, All(Any(str, unicode), Boolean())) }
