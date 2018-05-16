from six import string_types
from voluptuous import All, Any, Boolean, Coerce, Optional, Range, Required

# pylint: disable=E1120
# Action Options

def allocation_type():
    return { Optional('allocation_type', default='require'): All(
        Any(*string_types), Any('require', 'include', 'exclude')) }

def allow_ilm_indices():
    return { Optional('allow_ilm_indices', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def conditions():
    return {
        Optional('conditions'): {
            Optional('max_age'): Any(*string_types),
            Optional('max_docs'): Coerce(int),
            Optional('max_size'): Any(*string_types)
        }
    }

def continue_if_exception():
    return { Optional('continue_if_exception', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def count():
    return { Required('count'): All(Coerce(int), Range(min=0, max=10)) }

def delay():
    return {
        Optional('delay', default=0): All(
                Coerce(float), Range(min=0.0, max=3600.0)
            )
    }

def copy_aliases():
    return { Optional('copy_aliases', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def delete_after():
    return { Optional('delete_after', default=True): Any(bool, All(Any(*string_types), Boolean())) }

def delete_aliases():
    return { Optional('delete_aliases', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def disable_action():
    return { Optional('disable_action', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def extra_settings():
    return { Optional('extra_settings', default={}): dict }

def ignore_empty_list():
    return { Optional('ignore_empty_list', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def ignore_unavailable():
    return { Optional('ignore_unavailable', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def include_aliases():
    return { Optional('include_aliases', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def include_global_state(action):
    default = False
    if action == 'snapshot':
        default = True
    return { Optional('include_global_state', default=default): Any(bool, All(Any(*string_types), Boolean())) }

def index_settings():
    return { Required('index_settings'): {'index': dict} }

def indices():
    return { Optional('indices', default=None): Any(None, list) }

def key():
    return { Required('key'): Any(*string_types) }

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
    return { Optional('migration_prefix', default=''): Any(None, *string_types)}

def migration_suffix():
    return { Optional('migration_suffix', default=''): Any(None, *string_types)}

def name(action):
    if action in ['alias', 'create_index', 'rollover']:
        return { Required('name'): Any(*string_types) }
    elif action == 'snapshot':
        return {
            Optional('name', default='curator-%Y%m%d%H%M%S'): Any(*string_types)
        }
    elif action == 'restore':
        return { Optional('name'): Any(*string_types) }

def new_index():
    return { Optional('new_index', default=None): Any(None, *string_types) }

def node_filters():
    return {
        Optional('node_filters', default={}): {
          Optional('permit_masters', default=False): Any(bool, All(Any(*string_types), Boolean())),
          Optional('exclude_nodes', default=[]): Any(list, None)
        }
    }

def number_of_replicas():
    return { Optional('number_of_replicas', default=1): All(Coerce(int), Range(min=0, max=10)) }

def number_of_shards():
    return { Optional('number_of_shards', default=1): All(Coerce(int), Range(min=1, max=99)) }

def partial():
    return { Optional('partial', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def post_allocation():
    return {
        Optional('post_allocation', default={}): 
            Any(
                {}, 
                All(
                    {
                      Required('allocation_type', default='require'): All(Any(*string_types), Any('require', 'include', 'exclude')),
                      Required('key'): Any(*string_types),
                      Required('value', default=None): Any(None, *string_types)
                    }
                )
            )
    }

def preserve_existing():
    return { Optional('preserve_existing', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def refresh():
    return { Optional('refresh', default=True): Any(bool, All(Any(*string_types), Boolean())) }

def remote_aws_key():
    return { Optional('remote_aws_key', default=None): Any(None, *string_types) }

def remote_aws_secret_key():
    return { Optional('remote_aws_secret_key', default=None): Any(None, *string_types) }

def remote_aws_region():
    return { Optional('remote_aws_region', default=None): Any(None, *string_types) }

def remote_certificate():
    return { Optional('remote_certificate', default=None): Any(None, *string_types) }

def remote_client_cert():
    return { Optional('remote_client_cert', default=None): Any(None, *string_types) }

def remote_client_key():
    return { Optional('remote_client_key', default=None): Any(None, *string_types) }

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
    return { Optional('remote_ssl_no_validate', default=False): Any(bool, All(Any(*string_types), Boolean())) }

def remote_url_prefix():
    return { Optional('remote_url_prefix', default=''): Any(None, *string_types) }

def rename_pattern():
    return { Optional('rename_pattern'): Any(*string_types) }

def rename_replacement():
    return { Optional('rename_replacement'): Any(*string_types) }

def repository():
    return { Required('repository'): Any(*string_types) }

def request_body():
    return {
        Required('request_body'): {
            Optional('conflicts'): Any(*string_types),
            Optional('size'): Coerce(int),
            Required('source'): {
                Required('index'): Any(Any(*string_types), list),
                Optional('remote'): {
                    Optional('host'): Any(*string_types),
                    Optional('headers'): Any(*string_types),
                    Optional('username'): Any(*string_types),
                    Optional('password'): Any(*string_types),
                    Optional('socket_timeout'): Any(*string_types),
                    Optional('connect_timeout'): Any(*string_types),
                },
                Optional('size'): Coerce(int),
                Optional('type'): Any(Any(*string_types), list),
                Optional('query'): dict,
                Optional('sort'): dict,
                Optional('_source'): Any(Any(*string_types), list),
            },
            Required('dest'): {
                Required('index'): Any(*string_types),
                Optional('type'): Any(Any(*string_types), list),
                Optional('op_type'): Any(*string_types),
                Optional('version_type'): Any(*string_types),
                Optional('routing'): Any(*string_types),
                Optional('pipeline'): Any(*string_types),
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
    return { Required('shrink_node'): Any(*string_types) }

def shrink_prefix():
    return { Optional('shrink_prefix', default=''): Any(None, *string_types) }

def shrink_suffix():
    return { Optional('shrink_suffix', default='-shrink'): Any(None, *string_types) }

def skip_repo_fs_check():
    return { Optional('skip_repo_fs_check', default=False): Any(bool, All(Any(*string_types), Boolean())) }

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
    elif action == 'delete_snapshots':
        value = 300
    else:
        value = None

    return {
        Optional('timeout_override', default=value): Any(Coerce(int), None)
    }

def value():
    return { Required('value', default=None): Any(None, *string_types) }

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
    return { Optional('wait_for_completion', default=value): Any(bool, All(Any(*string_types), Boolean())) }

def wait_for_rebalance():
    return { Optional('wait_for_rebalance', default=True): Any(bool, All(Any(*string_types), Boolean())) }

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
    return { Optional('warn_if_no_indices', default=False): Any(bool, All(Any(*string_types), Boolean())) }
