---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_forcemerged.html
---

# forcemerged [filtertype_forcemerged]

```yaml
- filtertype: forcemerged
  max_num_segments: 2
  exclude: True
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices which have `max_num_segments` segments per shard, or fewer.  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

## Required settings [_required_settings_17]

* [max_num_segments](/reference/fe_max_num_segments.md)


## Optional settings [_optional_settings_24]

* [exclude](/reference/fe_exclude.md) (default is `True`)


