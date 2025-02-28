---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_use_age.html
---

# use_age [fe_use_age]

```yaml
- filtertype: count
  count: 10
  use_age: True
  source: creation_date
```

This setting allows filtering of indices by their age *after* other considerations.

The default value of this setting is `False`

::::{note}
Use of this setting requires the additional setting, [source](/reference/fe_source.md).
::::


::::{tip}
There are context-specific examples using `use_age` in the [count](/reference/filtertype_count.md) and [space](/reference/filtertype_space.md) documentation.
::::


