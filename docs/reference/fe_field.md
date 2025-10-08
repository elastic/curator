---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_field.html
---

# field [fe_field]

::::{note}
This setting is available in the [age](/reference/filtertype_age.md) filtertype, and any filter which has the [`use_age`](/reference/fe_use_age.md) setting. This setting is strictly optional.
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

The value of this setting must be a timestamp field name.  This field must be present in the indices being filtered or an exception will be raised, and execution will halt.

In Curator 5.3 and older, source `field_stats` uses the [Field Stats API](http://www.elastic.co/guide/en/elasticsearch/reference/5.6/search-field-stats.html) to calculate either the `min_value` or the `max_value` of the `field` as the [`stats_result`](/reference/fe_stats_result.md), and then use that value for age comparisons.  In 5.4 and above, even though it is still called `field_stats`, it uses an aggregation to calculate the same values, as the `field_stats` API is no longer used in Elasticsearch 6.x and up.

This setting is only used when [source](/reference/fe_source.md) is `field_stats`.

The default value for this setting is `@timestamp`.

