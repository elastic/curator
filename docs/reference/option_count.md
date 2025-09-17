---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_count.html
navigation_title: count
---

# Count option for replicas [option_count]

::::{note}
This setting is required when using the [replicas action](/reference/replicas.md).
::::


```yaml
action: replicas
description: >- Set the number of replicas per shard for selected
    indices to 'count'
options:
  count: ...
filters:
- filtertype: ...
```

The value for this setting is the number of replicas to assign to matching indices.

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

