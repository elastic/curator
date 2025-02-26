---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_stats_result.html
---

# stats_result [fe_stats_result]

::::{note}
This setting is only used with the [age](/reference/filtertype_age.md) filtertype.
::::


```yaml
 - filtertype: age
   source: field_stats
   direction: older
   unit: days
   unit_count: 3
   field: '@timestamp'
   stats_result: min_value
```

The value for this setting can be either `min_value` or `max_value`.  This setting is only used when [source](/reference/fe_source.md) is `field_stats`, and determines whether Curator will use the minimum or maximum value of [field](/reference/fe_field.md) for time calculations.

The default value for this setting is `min_value`.

