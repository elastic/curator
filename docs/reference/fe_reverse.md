---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_reverse.html
---

# reverse [fe_reverse]

::::{note}
This setting is used in the [count](/reference/filtertype_count.md) and [space](/reference/filtertype_space.md) filtertypes
::::


This setting affects the sort order of the indices.  `True` means reverse-alphabetical.  This means that if all index names share the same pattern with a date—​e.g. index-2016.03.01—​older indices will be selected first.

The default value of this setting is `True`.

This setting is ignored if [use_age](/reference/fe_use_age.md) is `True`.

::::{tip}
There are context-specific examples of how `reverse` works in the [count](/reference/filtertype_count.md) and [space](/reference/filtertype_space.md) documentation.
::::


