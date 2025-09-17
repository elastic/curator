---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_allocation_type.html
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

Read more about these settings in the [Elasticsearch documentation](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/shard-allocation-filtering.html).

The default value for this setting is `require`.

