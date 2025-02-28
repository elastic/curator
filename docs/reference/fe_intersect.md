---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_intersect.html
---

# intersect [fe_intersect]

::::{note}
This setting is only available in the [period](/reference/filtertype_age.md) filtertype. This setting is strictly optional.
::::


```yaml
 - filtertype: period
   source: field_stats
   direction: older
   intersect: true
   unit: weeks
   range_from: -1
   range_to: -1
   field: '@timestamp'
   stats_result: min_value
```

The value of this setting must be `True` or `False`.

`field_stats` uses an aggregation query to calculate either the `min_value` and the `max_value` of the [`field`](/reference/fe_field.md) as the [`stats_result`](/reference/fe_stats_result.md).  If `intersect` is `True`, then only indices where the `min_value` *and* the `max_value` are within the `range_from` and `range_to` (relative to `unit`) will match.  This means that either `min_value` or `max_value` can be used for [`stats_result`](/reference/fe_stats_result.md) when `intersect` is `True` with identical results.

This setting is only used when [source](/reference/fe_source.md) is `field_stats`.

The default value for this setting is `False`.

