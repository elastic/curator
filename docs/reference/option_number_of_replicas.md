---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_number_of_replicas.html
---

# number_of_replicas [option_number_of_replicas]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Set the number of replicas to 0.
options:
  shrink_node: DETERMINISTIC
  number_of_replicas: 0
filters:
  - filtertype: ...
```

The value of this setting determines the number of replica shards per primary shard in the target index.  The default value is `1`.

