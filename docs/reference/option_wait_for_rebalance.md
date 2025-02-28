---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_wait_for_rebalance.html
---

# wait_for_rebalance [option_wait_for_rebalance]

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
This setting is used by the [shrink](/reference/shrink.md) action.
::::


This setting must be `true` or `false`.

Setting this to `false` will result in the [shrink](/reference/shrink.md) action only checking that the index being shrunk has finished being relocated, and not continue to wait for the cluster to fully rebalance all shards.

The default value for this setting is `false`.

