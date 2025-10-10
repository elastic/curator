---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_range_from.html
---

# range_from [fe_range_from]

::::{note}
This setting is only used with the [period](/reference/filtertype_period.md) filtertype
::::


```yaml
 - filtertype: period
   source: name
   range_from: -1
   range_to: -1
   timestring: '%Y.%m.%d'
   unit: days
```

`range_from` and [`range_to`](/reference/fe_range_to.md) are counters of whole [units](/reference/fe_unit.md). A negative number indicates a whole unit in the past, while a positive number indicates a whole unit in the future. A `0` indicates the present unit.

Read more about this setting in context in the [period filtertype documentation](/reference/filtertype_period.md), including examples.

