---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_number_of_shards.html
---

# number_of_shards [option_number_of_shards]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Set the number of shards to 2.
options:
  shrink_node: DETERMINISTIC
  number_of_shards: 2
filters:
  - filtertype: ...
```

The value of this setting determines the number of primary shards in the target index.  The default value is `1`.

::::{important}
The value for `number_of_shards` must meet the following criteria:

* It must be lower than the number of primary shards in the source index.
* It must be a factor of the number of primary shards in the source index.

::::


