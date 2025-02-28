---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_mns.html
---

# max_num_segments [option_mns]

::::{note}
This setting is required when using the [forceMerge action](/reference/forcemerge.md).
::::


```yaml
action: forcemerge
description: >-
  Perform a forceMerge on selected indices to 'max_num_segments' per shard
options:
  max_num_segments: 2
  timeout_override: 21600
filters:
- filtertype: ...
```

The value for this setting is the cutoff number of segments per shard.  Indices which have more than this number of segments per shard will remain in the index list.

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

