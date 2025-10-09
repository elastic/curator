---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_allocation_type.html
navigation_title: allocation_type
---

# {{es}} Curator allocation action option: allocation_type [option_allocation_type]

::::{note}
This setting is used only when using the [allocation action](/reference/allocation.md)
::::


```yaml
action: allocation
description: "Apply shard allocation filtering rules to the specified indices"
options:
  key: ...
  value: ...
  allocation_type: ...
filters:
- filtertype: ...
```

The value of this setting must be one of `require`, `include`, or `exclude`.

Read more about these settings at [Index-level shard allocation](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/index-level-shard-allocation.md)

The default value for this setting is `require`.

