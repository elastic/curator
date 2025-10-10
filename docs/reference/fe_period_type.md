---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_period_type.html
---

# period_type [fe_period_type]

::::{note}
This setting is only used with the [period](/reference/filtertype_period.md) filtertype
::::


```yaml
 - filtertype: period
   period_type: absolute
   source: name
   timestring: '%Y.%m.%d'
   unit: months
   date_from: 2017.01
   date_from_format: '%Y.%m'
   date_to: 2017.01
   date_to_format: '%Y.%m'
```

The value for this setting must be either `relative` or `absolute`. The default value is `relative`.

