---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_max_num_segments.html
---

# max_num_segments [fe_max_num_segments]

::::{note}
This setting is only used with the [forcemerged](/reference/filtertype_forcemerged.md) filtertype.
::::


```yaml
- filtertype: forcemerged
  max_num_segments: 2
  exclude: True
```

The value for this setting is the cutoff number of segments per shard.  Indices which have this number of segments per shard, or fewer, will be actionable depending on the value of [exclude](/reference/fe_exclude.md), which is `True` by default for the [forcemerged](/reference/filtertype_forcemerged.md) filter type.

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

