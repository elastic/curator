---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_disk_space.html
---

# disk_space [fe_disk_space]

::::{note}
This setting is only used with the [space](/reference/filtertype_space.md) filtertype<br> and is a required setting.
::::


```yaml
- filtertype: space
  disk_space: 100
```

The value for this setting is a number of gigabytes.

Indices in excess of this number of gigabytes will be matched.

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

