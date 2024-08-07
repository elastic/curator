"""Action Option Schema definitions"""

from voluptuous import All, Any, Boolean, Coerce, Optional, Range, Required

# pylint: disable=E1120


def allocation_type():
    """
    :returns:
        {Optional('allocation_type', default='require'):
            All(Any(str), Any('require', 'include', 'exclude'))}
    """
    return {
        Optional('allocation_type', default='require'): All(
            Any(str), Any('require', 'include', 'exclude')
        )
    }


def allow_ilm_indices():
    """
    :returns:
        {Optional('allow_ilm_indices', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('allow_ilm_indices', default=False): Any(
            bool, All(Any(str), Boolean())
        )
    }


def conditions():
    """
    :returns:
        {Optional('conditions'): {Optional('max_age'):
            Any(str), Optional('max_docs'):
                Coerce(int), Optional('max_size'): Any(str)}}
    """
    return {
        Optional('conditions'): {
            Optional('max_age'): Any(str),
            Optional('max_docs'): Coerce(int),
            Optional('max_size'): Any(str),
        }
    }


def continue_if_exception():
    """
    :returns:
        {Optional('continue_if_exception', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('continue_if_exception', default=False): Any(
            bool, All(Any(str), Boolean())
        )
    }


def count():
    """
    :returns: {Required('count'): All(Coerce(int), Range(min=0, max=10))}
    """
    return {Required('count'): All(Coerce(int), Range(min=0, max=10))}


def delay():
    """
    :returns:
        {Optional('delay', default=0):
            All(Coerce(float), Range(min=0.0, max=3600.0))}
    """
    return {
        Optional('delay', default=0): All(Coerce(float), Range(min=0.0, max=3600.0))
    }


def c2f_index_settings():
    """
    Only for the :py:class:`~.curator.actions.Cold2Frozen` action

    :returns: {Optional('index_settings'):  Any(None, dict)}
    """
    return {Optional('index_settings', default=None): Any(None, dict)}


def c2f_ignore_index_settings():
    """
    Only for the :py:class:`~.curator.actions.Cold2Frozen` action

    :returns: {Optional('ignore_index_settings'):  Any(None, list)}
    """
    return {Optional('ignore_index_settings', default=None): Any(None, list)}


def copy_aliases():
    """
    :returns:
        {Optional('copy_aliases', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('copy_aliases', default=False): Any(bool, All(Any(str), Boolean()))
    }


def delete_after():
    """
    :returns:
        {Optional('delete_after', default=True):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {Optional('delete_after', default=True): Any(bool, All(Any(str), Boolean()))}


def delete_aliases():
    """
    :returns:
        {Optional('delete_aliases', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('delete_aliases', default=False): Any(bool, All(Any(str), Boolean()))
    }


def skip_flush():
    """
    :returns:
        {Optional('skip_flush', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {Optional('skip_flush', default=False): Any(bool, All(Any(str), Boolean()))}


def disable_action():
    """
    :returns:
        {Optional('disable_action', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('disable_action', default=False): Any(bool, All(Any(str), Boolean()))
    }


def extra_settings():
    """
    :returns: {Optional('extra_settings', default={}): dict}
    """
    return {Optional('extra_settings', default={}): dict}


def ignore_empty_list():
    """
    :returns:
        {Optional('ignore_empty_list', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('ignore_empty_list', default=False): Any(
            bool, All(Any(str), Boolean())
        )
    }


def ignore_existing():
    """
    :returns:
        {Optional('ignore_existing', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('ignore_existing', default=False): Any(bool, All(Any(str), Boolean()))
    }


def ignore_unavailable():
    """
    :returns:
        {Optional('ignore_unavailable', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('ignore_unavailable', default=False): Any(
            bool, All(Any(str), Boolean())
        )
    }


def include_aliases():
    """
    :returns:
        {Optional('include_aliases', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('include_aliases', default=False): Any(bool, All(Any(str), Boolean()))
    }


def include_global_state(action):
    """
    :returns:
        {Optional('include_global_state', default=default):
            Any(bool, All(Any(str), Boolean()))}
    """
    default = False
    if action == 'snapshot':
        default = True
    return {
        Optional('include_global_state', default=default): Any(
            bool, All(Any(str), Boolean())
        )
    }


def index_settings():
    """
    :returns: {Required('index_settings'): {'index': dict}}
    """
    return {Required('index_settings'): {'index': dict}}


def indices():
    """
    :returns: {Optional('indices', default=None): Any(None, list)}
    """
    return {Optional('indices', default=None): Any(None, list)}


def key():
    """
    :returns: {Required('key'): Any(str)}
    """
    return {Required('key'): Any(str)}


def max_num_segments():
    """
    :returns:
        {Required('max_num_segments'):
            All(Coerce(int), Range(min=1, max=32768))}
    """
    return {Required('max_num_segments'): All(Coerce(int), Range(min=1, max=32768))}


# pylint: disable=unused-argument
def max_wait(action):
    """
    :returns: {Optional('max_wait', default=defval): Any(-1, Coerce(int), None)}
    """
    # The separation is here in case I want to change defaults later...
    defval = -1
    # if action in ['allocation', 'cluster_routing', 'replicas']:
    #     defval = -1
    # elif action in ['restore', 'snapshot', 'reindex', 'shrink']:
    #     defval = -1
    return {Optional('max_wait', default=defval): Any(-1, Coerce(int), None)}


def migration_prefix():
    """
    :returns: {Optional('migration_prefix', default=''): Any(None, str)}
    """
    return {Optional('migration_prefix', default=''): Any(None, str)}


def migration_suffix():
    """
    :returns: {Optional('migration_suffix', default=''): Any(None, str)}
    """
    return {Optional('migration_suffix', default=''): Any(None, str)}


def name(action):
    """
    :returns: The proper name based on what action it is:
        ``alias``, ``create_index``, ``rollover``: {Required('name'): Any(str)}
        ``snapshot``: {Optional('name', default='curator-%Y%m%d%H%M%S'): Any(str)}
        ``restore``: {Optional('name'): Any(str)}
    """
    if action in ['alias', 'create_index', 'rollover']:
        return {Required('name'): Any(str)}
    if action == 'snapshot':
        return {Optional('name', default='curator-%Y%m%d%H%M%S'): Any(str)}
    if action == 'restore':
        return {Optional('name'): Any(str)}


def new_index():
    """
    :returns: {Optional('new_index', default=None): Any(None, str)}
    """
    return {Optional('new_index', default=None): Any(None, str)}


def node_filters():
    """
    :returns: A :py:class:`voluptuous.schema_builder.Schema` object.
        See code for more details.
    """
    return {
        Optional('node_filters', default={}): {
            Optional('permit_masters', default=False): Any(
                bool, All(Any(str), Boolean())
            ),
            Optional('exclude_nodes', default=[]): Any(list, None),
        }
    }


def number_of_replicas():
    """
    :returns:
        {Optional('number_of_replicas', default=1):
            All(Coerce(int), Range(min=0, max=10))}
    """
    return {
        Optional('number_of_replicas', default=1): All(
            Coerce(int), Range(min=0, max=10)
        )
    }


def number_of_shards():
    """
    :returns:
        {Optional('number_of_shards', default=1):
            All(Coerce(int), Range(min=1, max=99))}
    """
    return {
        Optional('number_of_shards', default=1): All(Coerce(int), Range(min=1, max=99))
    }


def partial():
    """
    :returns:
        {Optional('partial', default=False): Any(bool, All(Any(str), Boolean()))}
    """
    return {Optional('partial', default=False): Any(bool, All(Any(str), Boolean()))}


def post_allocation():
    """
    :returns: A :py:class:`voluptuous.schema_builder.Schema` object.
        See code for more details.
    """
    return {
        Optional('post_allocation', default={}): Any(
            {},
            All(
                {
                    Required('allocation_type', default='require'): All(
                        Any(str), Any('require', 'include', 'exclude')
                    ),
                    Required('key'): Any(str),
                    Required('value', default=None): Any(None, str),
                }
            ),
        )
    }


def preserve_existing():
    """
    :returns:
        {Optional('preserve_existing', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('preserve_existing', default=False): Any(
            bool, All(Any(str), Boolean())
        )
    }


def refresh():
    """
    :returns:
        {Optional('refresh', default=True): Any(bool, All(Any(str), Boolean()))}
    """
    return {Optional('refresh', default=True): Any(bool, All(Any(str), Boolean()))}


def remote_certificate():
    """
    :returns: {Optional('remote_certificate', default=None): Any(None, str)}
    """
    return {Optional('remote_certificate', default=None): Any(None, str)}


def remote_client_cert():
    """
    :returns: {Optional('remote_client_cert', default=None): Any(None, str)}
    """
    return {Optional('remote_client_cert', default=None): Any(None, str)}


def remote_client_key():
    """
    :returns: {Optional('remote_client_key', default=None): Any(None, str)}
    """
    return {Optional('remote_client_key', default=None): Any(None, str)}


def remote_filters():
    """
    :returns: A :py:class:`voluptuous.schema_builder.Schema` object.
        See code for more details.
    """
    # This is really just a basic check here.  The real check is in the
    # validate_actions() method in utils.py
    return {
        Optional(
            'remote_filters',
            default=[
                {
                    'filtertype': 'pattern',
                    'kind': 'regex',
                    'value': '.*',
                    'exclude': True,
                }
            ],
        ): Any(list, None)
    }


def rename_pattern():
    """
    :returns: {Optional('rename_pattern'): Any(str)}
    """
    return {Optional('rename_pattern'): Any(str)}


def rename_replacement():
    """
    :returns: {Optional('rename_replacement'): Any(str)}
    """
    return {Optional('rename_replacement'): Any(str)}


def repository():
    """
    :returns: {Required('repository'): Any(str)}
    """
    return {Required('repository'): Any(str)}


def request_body():
    """
    :returns: A :py:class:`voluptuous.schema_builder.Schema` object.
        See code for more details.
    """
    return {
        Required('request_body'): {
            Optional('conflicts'): Any('proceed', 'abort'),
            Optional('max_docs'): Coerce(int),
            Required('source'): {
                Required('index'): Any(Any(str), list),
                Optional('query'): dict,
                Optional('remote'): {
                    Optional('host'): Any(str),
                    Optional('username'): Any(str),
                    Optional('password'): Any(str),
                    Optional('socket_timeout'): Any(str),
                    Optional('connect_timeout'): Any(str),
                    Optional('headers'): Any(str),
                },
                Optional('size'): Coerce(int),
                Optional('_source'): Any(bool, Boolean()),
            },
            Required('dest'): {
                Required('index'): Any(str),
                Optional('version_type'): Any(
                    'internal', 'external', 'external_gt', 'external_gte'
                ),
                Optional('op_type'): Any(str),
                Optional('pipeline'): Any(str),
            },
            Optional('script'): {
                Optional('source'): Any(str),
                Optional('lang'): Any('painless', 'expression', 'mustache', 'java'),
            },
        }
    }


def requests_per_second():
    """
    :returns:
        {Optional('requests_per_second', default=-1): Any(-1, Coerce(int), None)}
    """
    return {Optional('requests_per_second', default=-1): Any(-1, Coerce(int), None)}


def retry_count():
    """
    :returns:
        {Optional('retry_count', default=3):
            All(Coerce(int), Range(min=0, max=100))}
    """
    return {Optional('retry_count', default=3): All(Coerce(int), Range(min=0, max=100))}


def retry_interval():
    """
    :returns:
        {Optional('retry_interval', default=120):
            All(Coerce(int), Range(min=1, max=600))}
    """
    return {
        Optional('retry_interval', default=120): All(Coerce(int), Range(min=1, max=600))
    }


def routing_type():
    """
    :returns: {Required('routing_type'): Any('allocation', 'rebalance')}
    """
    return {Required('routing_type'): Any('allocation', 'rebalance')}


def cluster_routing_setting():
    """
    :returns: {Required('setting'): Any('enable')}
    """
    return {Required('setting'): Any('enable')}


def cluster_routing_value():
    """
    :returns:
        {Required('value'):
            Any('all', 'primaries', 'none', 'new_primaries', 'replicas')}
    """
    return {
        Required('value'): Any('all', 'primaries', 'none', 'new_primaries', 'replicas')
    }


def search_pattern():
    """
    :returns: {Optional('search_pattern', default='_all'): Any(str)}
    """
    return {Optional('search_pattern', default='_all'): Any(str)}


def shrink_node():
    """
    :returns: {Required('shrink_node'): Any(str)}
    """
    return {Required('shrink_node'): Any(str)}


def shrink_prefix():
    """
    :returns: {Optional('shrink_prefix', default=''): Any(None, str)}
    """
    return {Optional('shrink_prefix', default=''): Any(None, str)}


def shrink_suffix():
    """
    :returns: {Optional('shrink_suffix', default='-shrink'): Any(None, str)}
    """
    return {Optional('shrink_suffix', default='-shrink'): Any(None, str)}


def skip_repo_fs_check():
    """
    :returns:
        {Optional('skip_repo_fs_check', default=True):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('skip_repo_fs_check', default=True): Any(
            bool, All(Any(str), Boolean())
        )
    }


def slices():
    """
    :returns:
        {Optional('slices', default=1):
            Any(All(Coerce(int), Range(min=1, max=500)), None)}
    """
    return {
        Optional('slices', default=1): Any(
            All(Coerce(int), Range(min=1, max=500)), None
        )
    }


def timeout(action):
    """
    :returns: {Optional('timeout', default=defval): Any(Coerce(int), None)}
    """
    # if action == 'reindex':
    defval = 60
    return {Optional('timeout', default=defval): Any(Coerce(int), None)}


def timeout_override(action):
    """
    :returns:
        {Optional('timeout_override', default=defval): Any(Coerce(int), None)}
        where ``defval`` is determined by the action.

        ``['forcemerge', 'restore', 'snapshot']`` = ``21600``

        ``close`` = ``180``

        ``delete_snapshots`` = ``300``
    """
    if action in ['forcemerge', 'restore', 'snapshot']:
        defval = 21600
    elif action == 'close':
        defval = 180
    elif action == 'delete_snapshots':
        defval = 300
    else:
        defval = None
    return {Optional('timeout_override', default=defval): Any(Coerce(int), None)}


def value():
    """
    :returns: {Required('value', default=None): Any(None, str)}
    """
    return {Required('value', default=None): Any(None, str)}


def wait_for_active_shards(action):
    """
    :returns:
        {Optional('wait_for_active_shards', default=defval):
            Any(Coerce(int), 'all', None)}
            where ``defval`` defaults to 0, but changes to 1 for the ``reindex`` and
            ``shrink`` actions.
    """
    defval = 0
    if action in ['reindex', 'shrink']:
        defval = 1
    return {
        Optional('wait_for_active_shards', default=defval): Any(
            Coerce(int), 'all', None
        )
    }


def wait_for_completion(action):
    """
    :returns:
        {Optional('wait_for_completion', default=defval):
            Any(bool, All(Any(str), Boolean()))}
            where ``defval`` defaults to True, but changes to False if action is
            ``allocation``, ``cluster_routing``, or ``replicas``.
    """
    # if action in ['cold2frozen', 'reindex', 'restore', 'snapshot']:
    defval = True
    if action in ['allocation', 'cluster_routing', 'replicas']:
        defval = False
    return {
        Optional('wait_for_completion', default=defval): Any(
            bool, All(Any(str), Boolean())
        )
    }


def wait_for_rebalance():
    """
    :returns:
        {Optional('wait_for_rebalance', default=True):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('wait_for_rebalance', default=True): Any(
            bool, All(Any(str), Boolean())
        )
    }


def wait_interval(action):
    """
    :returns:
        {Optional('wait_interval', default=defval):
            Any(All(Coerce(int), Range(min=minval, max=maxval)), None)}
            where ``minval`` = ``1``, ``maxval`` = ``30``, and ``defval`` is ``3``,
            unless the action is one of
            ``['restore', 'snapshot', 'reindex', 'shrink']``, and then ``defval``
            is ``9``.
    """
    minval = 1
    maxval = 30
    # if action in ['allocation', 'cluster_routing', 'replicas']:
    defval = 3
    if action in ['restore', 'snapshot', 'reindex', 'shrink']:
        defval = 9
    return {
        Optional('wait_interval', default=defval): Any(
            All(Coerce(int), Range(min=minval, max=maxval)), None
        )
    }


def warn_if_no_indices():
    """
    :returns:
        {Optional('warn_if_no_indices', default=False):
            Any(bool, All(Any(str), Boolean()))}
    """
    return {
        Optional('warn_if_no_indices', default=False): Any(
            bool, All(Any(str), Boolean())
        )
    }
