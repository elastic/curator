---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_allocation_type.html
navigation_title: allocation_type
---

# {{es}} Curator allocation filter setting: allocation_type [fe_allocation_type]

::::{note}
This setting is used only when using the [allocated](/reference/filtertype_allocated.md) filter.
::::


```yaml
- filtertype: allocated
  key: ...
  value: ...
  allocation_type: require
  exclude: True
```

The value of this setting must be one of `require`, `include`, or `exclude`.

Read more about these settings in [Index-level shard allocation](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/index-level-shard-allocation.md).

The default value for this setting is `require`.

