"""Set up voluptuous Schema defaults for various actions"""
from voluptuous import Schema
from curator.defaults import option_defaults

## Methods for building the schema
def action_specific(action):
    """
    :param action: The name of an action
    :type action: str

    :returns: A :py:class:`list` containing one or more
        :py:class:`~.voluptuous.schema_builder.Optional` or
        :py:class:`~.voluptuous.schema_builder.Required` options from
        :py:mod:`~.curator.defaults.option_defaults`, defining acceptable values for each for the
        given ``action``
    :rtype: list
    """
    options = {
        'alias' : [
            option_defaults.name(action),
            option_defaults.warn_if_no_indices(),
            option_defaults.extra_settings(),
        ],
        'allocation' : [
            option_defaults.search_pattern(),
            option_defaults.key(),
            option_defaults.value(),
            option_defaults.allocation_type(),
            option_defaults.wait_for_completion(action),
            option_defaults.wait_interval(action),
            option_defaults.max_wait(action),
        ],
        'close' : [
            option_defaults.search_pattern(),
            option_defaults.delete_aliases(),
            option_defaults.skip_flush(),
        ],
        'cluster_routing' : [
            option_defaults.routing_type(),
            option_defaults.cluster_routing_setting(),
            option_defaults.cluster_routing_value(),
            option_defaults.wait_for_completion(action),
            option_defaults.wait_interval(action),
            option_defaults.max_wait(action),
        ],
        'cold2frozen' : [
            option_defaults.search_pattern(),
            option_defaults.c2f_index_settings(),
            option_defaults.c2f_ignore_index_settings(),
            option_defaults.wait_for_completion('cold2frozen'),
        ],
        'create_index' : [
            option_defaults.name(action),
            option_defaults.ignore_existing(),
            option_defaults.extra_settings(),
        ],
        'delete_indices' : [
            option_defaults.search_pattern(),
        ],
        'delete_snapshots' : [
            option_defaults.repository(),
            option_defaults.retry_interval(),
            option_defaults.retry_count(),
        ],
        'forcemerge' : [
            option_defaults.search_pattern(),
            option_defaults.delay(),
            option_defaults.max_num_segments(),
        ],
        'index_settings' : [
            option_defaults.search_pattern(),
            option_defaults.index_settings(),
            option_defaults.ignore_unavailable(),
            option_defaults.preserve_existing(),
        ],
        'open' : [
            option_defaults.search_pattern(),
        ],
        'reindex' : [
            option_defaults.request_body(),
            option_defaults.refresh(),
            option_defaults.requests_per_second(),
            option_defaults.slices(),
            option_defaults.timeout(action),
            option_defaults.wait_for_active_shards(action),
            option_defaults.wait_for_completion(action),
            option_defaults.wait_interval(action),
            option_defaults.max_wait(action),
            option_defaults.remote_certificate(),
            option_defaults.remote_client_cert(),
            option_defaults.remote_client_key(),
            option_defaults.remote_filters(),
            option_defaults.migration_prefix(),
            option_defaults.migration_suffix(),
        ],
        'replicas' : [
            option_defaults.search_pattern(),
            option_defaults.count(),
            option_defaults.wait_for_completion(action),
            option_defaults.wait_interval(action),
            option_defaults.max_wait(action),
        ],
        'rollover' : [
            option_defaults.name(action),
            option_defaults.new_index(),
            option_defaults.conditions(),
            option_defaults.extra_settings(),
            option_defaults.wait_for_active_shards(action),
        ],
        'restore' : [
            option_defaults.repository(),
            option_defaults.name(action),
            option_defaults.indices(),
            option_defaults.ignore_unavailable(),
            option_defaults.include_aliases(),
            option_defaults.include_global_state(action),
            option_defaults.partial(),
            option_defaults.rename_pattern(),
            option_defaults.rename_replacement(),
            option_defaults.extra_settings(),
            option_defaults.wait_for_completion(action),
            option_defaults.wait_interval(action),
            option_defaults.max_wait(action),
            option_defaults.skip_repo_fs_check(),
        ],
        'snapshot' : [
            option_defaults.search_pattern(),
            option_defaults.repository(),
            option_defaults.name(action),
            option_defaults.ignore_unavailable(),
            option_defaults.include_global_state(action),
            option_defaults.partial(),
            option_defaults.wait_for_completion(action),
            option_defaults.wait_interval(action),
            option_defaults.max_wait(action),
            option_defaults.skip_repo_fs_check(),
        ],
        'shrink' : [
            option_defaults.search_pattern(),
            option_defaults.shrink_node(),
            option_defaults.node_filters(),
            option_defaults.number_of_shards(),
            option_defaults.number_of_replicas(),
            option_defaults.shrink_prefix(),
            option_defaults.shrink_suffix(),
            option_defaults.copy_aliases(),
            option_defaults.delete_after(),
            option_defaults.post_allocation(),
            option_defaults.wait_for_active_shards(action),
            option_defaults.extra_settings(),
            option_defaults.wait_for_completion(action),
            option_defaults.wait_for_rebalance(),
            option_defaults.wait_interval(action),
            option_defaults.max_wait(action),
        ],
    }
    return options[action]

def get_schema(action):
    """
    Return a :py:class:`~.voluptuous.schema_builder.Schema` of acceptable options and their default
    values as returned by :py:func:`action_specific`, passing along the value of ``action``.

    :param action: The name of an action
    :type action: str

    :returns: A valid :py:class:`~.voluptuous.schema_builder.Schema` of the options for ``action``
    """
    options = {}
    defaults = [
        option_defaults.allow_ilm_indices(),
        option_defaults.continue_if_exception(),
        option_defaults.disable_action(),
        option_defaults.ignore_empty_list(),
        option_defaults.timeout_override(action),
    ]
    for each in defaults:
        options.update(each)
    for each in action_specific(action):
        options.update(each)
    return Schema(options)
