---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_threshold_behavior.html
---

# threshold_behavior [fe_threshold_behavior]

::::{note}
This setting is only available in the [space](/reference/filtertype_space.md) filtertype. This setting is optional, and defaults to `greater_than` to preserve backwards compatability.
::::


```yaml
- filtertype: space
  disk_space: 5
  threshold_behavior: less_than
```

The value for this setting is `greater_than` (default) or `less_than`.

When set to `less_than`, indices in less than `disk_space` gigabytes will be matched. When set to `greater_than` (default), indices larger than `disk_space` will be matched.

