---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_post_allocation.html
---

# post_allocation [option_post_allocation]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Apply shard routing allocation to target indices.
options:
  shrink_node: DETERMINISTIC
  post_allocation:
    allocation_type: include
    key: node_tag
    value: cold
filters:
  - filtertype: ...
```

The only permitted subkeys for `post_allocation` are the same options used by the [allocation](/reference/allocation.md) action: [allocation_type](/reference/option_allocation_type.md), [key](/reference/option_key.md), and [value](/reference/option_value.md).

If present, these values will be use to apply shard routing allocation to the target index after shrinking.

There is no default value for `post_allocation`.

