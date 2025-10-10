---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_date_from_format.html
---

# date_from_format [fe_date_from_format]

::::{note}
This setting is only used with the [period](/reference/filtertype_period.md) filtertype<br> when [period_type](/reference/fe_period_type.md) is `absolute`.
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

The value for this setting should be an strftime string which corresponds to the date in [`date_from`](/reference/fe_date_from.md):

The identifiers that Curator currently recognizes include:

| Unit | Value | Note |
| --- | --- | --- |
| `%Y` | 4 digit year |  |
| `%G` | 4 digit year | use instead of `%Y` when doing ISO Week calculations |
| `%y` | 2 digit year |  |
| `%m` | 2 digit month |  |
| `%W` | 2 digit week of the year |  |
| `%V` | 2 digit week of the year | use instead of `%W` when doing ISO Week calculations |
| `%d` | 2 digit day of the month |  |
| `%H` | 2 digit hour | 24 hour notation |
| `%M` | 2 digit minute |  |
| `%S` | 2 digit second |  |
| `%j` | 3 digit day of the year |  |

