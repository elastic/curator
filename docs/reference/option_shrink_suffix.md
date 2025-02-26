---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_shrink_suffix.html
---

# shrink_suffix [option_shrink_suffix]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Prepend target index names with 'foo-' and append a suffix of '-shrink'
options:
  shrink_node: DETERMINISTIC
  shrink_prefix: 'foo-'
  shrink_suffix: '-shrink'
filters:
  - filtertype: ...
```

The default value for this setting is `-shrink`.

The value of this setting will be appended to target index names.  If the source index were `index`, and the `shrink_prefix` were `foo-`, and the `shrink_suffix` were `-shrink`, the resulting target index name would be `foo-index-shrink`.

