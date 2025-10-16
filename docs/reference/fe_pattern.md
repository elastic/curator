---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_pattern.html
navigation_title: pattern
---

# pattern filter element [fe_pattern]

::::{note}
This setting is only used with the [count](/reference/filtertype_count.md) filtertype
::::


```yaml
- filtertype: count
  count: 1
  pattern: '^(.*)-\d{6}$'
  reverse: true
```

This particular example will match indices following the basic rollover pattern of `indexname-######`, and keep the highest numbered index for each group.

For example, given indices `a-000001`, `a-000002`, `a-000003` and `b-000006`, and `b-000007`, the indices will would be matched are `a-000003` and `b-000007`. Indices that do not match the regular expression in `pattern` will be automatically excluded.

This is particularly useful with indices created and managed using the [Rollover API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-rollover), as you can select only the active indices with the above example ([`exclude`](/reference/fe_exclude.md) defaults to `False`). Setting [`exclude`](/reference/fe_exclude.md) to `True` with the above example will *remove* the active rollover indices, leaving only those which have been rolled-over.

While this is perhaps most useful for the aforementioned scenario, it can also be used with age-based indices as well.

Items will remain in the actionable list depending on the value of [exclude](/reference/fe_exclude.md), and [reverse](/reference/fe_reverse.md).

There is no default value. The value must include a capture group, defined by parenthesis, or left empty.  If a value is provided, and there is no capture group, and exception will be raised and execution will halt.

