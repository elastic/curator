---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_delay.html
---

# delay [option_delay]

::::{note}
This setting is only used by the [forceMerge action](/reference/forcemerge.md), and is optional.
::::


```yaml
action: forcemerge
description: >-
  Perform a forceMerge on selected indices to 'max_num_segments' per shard
options:
  max_num_segments: 2
  timeout_override: 21600
  delay: 120
filters:
- filtertype: ...
```

The value for this setting is the number of seconds to delay between forceMerging indices, to allow the cluster to quiesce.

There is no default value.

