---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_week_starts_on.html
---

# week_starts_on [fe_week_starts_on]

::::{note}
This setting is only used with the [period](/reference/filtertype_period.md) filtertype<br> when [period_type](/reference/fe_period_type.md) is `relative`.
::::


```yaml
 - filtertype: period
   source: name
   range_from: -1
   range_to: -1
   timestring: '%Y.%m.%d'
   unit: weeks
   week_starts_on: sunday
```

The value of this setting indicates whether weeks should be measured starting on `sunday` or `monday`.  Though Monday is the ISO standard, Sunday is frequently preferred.

This setting is only used when [unit](/reference/fe_unit.md) is set to `weeks`.

The default value for this setting is `sunday`.

