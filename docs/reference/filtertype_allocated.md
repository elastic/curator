---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_allocated.html
---

# allocated [filtertype_allocated]

```yaml
- filtertype: allocated
  key: ...
  value: ...
  allocation_type:
  exclude: True
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices based on their shard routing allocation settings.  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

By default the indices matched by the `allocated` filter will be excluded since the `exclude` setting defaults to `True`.

To include matching indices rather than exclude, set the `exclude` setting to `False`.

## Required settings [_required_settings_15]

* [key](/reference/fe_key.md)
* [value](/reference/fe_value.md)


## Optional settings [_optional_settings_20]

* [allocation_type](/reference/fe_allocation_type.md)
* [exclude](/reference/fe_exclude.md) (default is `True`)


