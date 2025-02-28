---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_count.html
---

# count [fe_count]

::::{note}
This setting is only used with the [count](/reference/filtertype_count.md) filtertype<br> and is a required setting.
::::


```yaml
- filtertype: count
  count: 10
```

The value for this setting is a number of indices or snapshots to match.

Items will remain in the actionable list depending on the value of [exclude](/reference/fe_exclude.md), and [reverse](/reference/fe_reverse.md).

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

