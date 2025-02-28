---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/shrink.html
---

# Shrink [shrink]

```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Delete source index after successful shrink, then reroute the shrunk
  index with the provided parameters.
options:
  ignore_empty_list: True
  shrink_node: DETERMINISTIC
  node_filters:
    permit_masters: False
    exclude_nodes: ['not_this_node']
  number_of_shards: 1
  number_of_replicas: 1
  shrink_prefix:
  shrink_suffix: '-shrink'
  delete_after: True
  post_allocation:
    allocation_type: include
    key: node_tag
    value: cold
  wait_for_active_shards: 1
  extra_settings:
    settings:
      index.codec: best_compression
  wait_for_completion: True
  wait_for_rebalance: True
  wait_interval: 9
  max_wait: -1
filters:
  - filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


Shrinking an index is a good way to reduce the total shard count in your cluster. [Several conditions need to be met](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink) in order for index shrinking to take place:

* The index must be marked as read-only
* A (primary or replica) copy of every shard in the index must be relocated to the same node
* The cluster must have health `green`
* The target index must not exist
* The number of primary shards in the target index must be a factor of the number of primary shards in the source index.
* The source index must have more primary shards than the target index.
* The index must not contain more than 2,147,483,519 documents in total across all shards that will be shrunk into a single shard on the target index as this is the maximum number of docs that can fit into a single shard.
* The node handling the shrink process must have sufficient free disk space to accommodate a second copy of the existing index.

Curator will try to meet these conditions.  If it is unable to meet them all, it will not perform a shrink operation.

This action will shrink indices to the target index, the name of which is the value of [shrink_prefix](/reference/option_shrink_prefix.md) + the source index name + [shrink_suffix](/reference/option_shrink_suffix.md). The resulting index will have [number_of_shards](/reference/option_number_of_shards.md) primary shards, and [number_of_replicas](/reference/option_number_of_replicas.md) replica shards.

The shrinking will take place on the node identified by [shrink_node](/reference/option_shrink_node.md), unless `DETERMINISTIC` is specified, in which case Curator will evaluate all of the nodes to determine which one has the most free space.  If multiple indices are identified for shrinking by the filter block, and `DETERMINISTIC` is specified, the node selection process will be repeated for each successive index, preventing all of the space being consumed on a single node.

By default, Curator will delete the source index after a successful shrink. This can be disabled by setting [delete_after](/reference/option_delete_after.md) to `False`.  If the source index, is not deleted after a successful shrink, Curator will remove the read-only setting and the shard allocation routing applied to the source index to put it on the shrink node.  Curator will wait for the shards to stop rerouting before continuing.

The [post_allocation](/reference/option_post_allocation.md) option applies to the target index after the shrink is complete.  If set, this shard allocation routing will be applied (after a successful shrink) and Curator will wait for all shards to stop rerouting before continuing.

The only [extra_settings](/reference/option_extra_settings.md) which are acceptable are `settings` and `aliases`. Please note that in the example above, while `best_compression` is being applied to the new index, it will not take effect until new writes are made to the index, such as when [force-merging](/reference/forcemerge.md) the shard to a single segment.

The other options are usually okay to leave at the defaults, but feel free to change them as needed.

## Required settings [_required_settings_11]

* [shrink_node](/reference/option_shrink_node.md)


## Optional settings [_optional_settings_16]

* [search_pattern](/reference/option_search_pattern.md)
* [continue_if_exception](/reference/option_continue.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [copy_aliases](/reference/option_copy_aliases.md)
* [delete_after](/reference/option_delete_after.md)
* [disable_action](/reference/option_disable.md)
* [extra_settings](/reference/option_extra_settings.md)
* [node_filters](/reference/option_node_filters.md)
* [number_of_shards](/reference/option_number_of_shards.md)
* [number_of_replicas](/reference/option_number_of_replicas.md)
* [post_allocation](/reference/option_post_allocation.md)
* [shrink_prefix](/reference/option_shrink_prefix.md)
* [shrink_suffix](/reference/option_shrink_suffix.md)
* [timeout_override](/reference/option_timeout_override.md)
* [wait_for_active_shards](/reference/option_wait_for_active_shards.md)
* [wait_for_completion](/reference/option_wfc.md)
* [wait_for_rebalance](/reference/option_wait_for_rebalance.md)
* [max_wait](/reference/option_max_wait.md)
* [wait_interval](/reference/option_wait_interval.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_shrink.md).
::::



