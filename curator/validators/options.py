from voluptuous import *
from ..defaults import settings

### Schema information ###

def allocation_type():
    return { Optional('allocation_type', default='require'): All(
        Any(str, unicode), Any('require', 'include', 'exclude')) }

def continue_if_exception():
    return { Optional('continue_if_exception', default=False): Boolean() }

def count():
    return { Required('count'): All(Coerce(int), Range(min=0, max=10)) }

def delay():
    return {
        Optional('delay', default=0): All(
                Coerce(float), Range(min=0.0, max=3600.0)
            )
    }

def delete_aliases():
    return { Optional('delete_aliases', default=False): Boolean() }

def disable_action():
    return { Optional('disable_action', default=False): Boolean() }

def extra_settings():
    return { Optional('extra_settings', default={}): dict }

def ignore_empty_list():
    return { Optional('ignore_empty_list', default=False): Boolean() }

def ignore_unavailable():
    return { Optional('ignore_unavailable', default=False): Boolean() }

def include_aliases():
    return { Optional('include_aliases', default=False): Boolean() }

def include_global_state():
    return { Optional('include_global_state', default=True): Boolean() }

def indices():
    return { Optional('indices', default=None): Any(None, list) }

def key():
    return { Required('key'): Any(str, unicode) }

def max_num_segments():
    return {
        Required('max_num_segments'): All(Coerce(int), Range(min=1, max=32768))
    }

def name(action):
    if action in ['alias', 'create_index']:
        return { Required('name'): Any(str, unicode) }
    elif action == 'snapshot':
        return {
            Optional('name', default='curator-%Y%m%d%H%M%S'): Any(str, unicode)
        }
    elif action == 'restore':
        return { Optional('name'): Any(str, unicode) }

def partial():
    return { Optional('partial', default=False): Boolean() }

def rename_pattern():
    return { Optional('rename_pattern'): Any(str, unicode) }

def rename_replacement():
    return { Optional('rename_replacement'): Any(str, unicode) }

def repository():
    return { Required('repository'): Any(str, unicode) }

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

def skip_repo_fs_check():
    return { Optional('skip_repo_fs_check', default=False): Boolean() }

def timeout_override(action):
    if action in ['forcemerge', 'restore', 'snapshot']:
        return {
            Optional('timeout_override', default=21600): Any(Coerce(int), None)
        }
    if action in ['close']:
        # This is due to the synced flush operation before closing.
        return {
            Optional('timeout_override', default=180): Any(Coerce(int), None)
        }
    else:
        return {
            Optional('timeout_override', default=None): Any(Coerce(int), None)
        }

def value():
    return { Required('value'): Any(str, unicode) }



def wait_for_completion(action):
    if action in ['allocation', 'cluster_routing', 'replicas']:
        return { Optional('wait_for_completion', default=False): Boolean() }
    elif action in ['restore', 'snapshot']:
        return { Optional('wait_for_completion', default=True): Boolean() }

## Methods for building the schema
def action_specific(action):
    options = {
        'alias' : [
            name(action),
            extra_settings(),
        ],
        'allocation' : [
            key(),
            value(),
            allocation_type(),
            wait_for_completion(action),
        ],
        'close' : [ delete_aliases() ],
        'cluster_routing' : [
            routing_type(),
            cluster_routing_setting(),
            cluster_routing_value(),
            wait_for_completion(action),
        ],
        'create_index' : [
            name(action),
            extra_settings(),
        ],
        'delete_indices' : [],
        'delete_snapshots' : [
            repository(),
            retry_interval(),
            retry_count(),
        ],
        'forcemerge' : [
            delay(),
            max_num_segments(),
        ],
        'open' : [],
        'replicas' : [
            count(),
            wait_for_completion(action),
        ],
        'restore' : [
            repository(),
            name(action),
            indices(),
            ignore_unavailable(),
            include_aliases(),
            include_global_state(),
            partial(),
            rename_pattern(),
            rename_replacement(),
            extra_settings(),
            wait_for_completion(action),
            skip_repo_fs_check(),
        ],
        'snapshot' : [
            repository(),
            name(action),
            ignore_unavailable(),
            include_global_state(),
            partial(),
            wait_for_completion(action),
            skip_repo_fs_check(),
        ],
    }
    return options[action]

def get_schema(action):
    # Appending the options dictionary seems to be the best way, since the
    # "Required" and "Optional" elements are hashes themselves.
    options = {}
    defaults = [
        continue_if_exception(),
        disable_action(),
        ignore_empty_list(),
        timeout_override(action),
    ]
    for each in defaults:
        options.update(each)
    for each in action_specific(action):
        options.update(each)
    return Schema(options)
