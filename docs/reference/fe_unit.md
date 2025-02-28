---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_unit.html
---

# unit [fe_unit]

::::{note}
This setting is used with the [age](/reference/filtertype_age.md) filtertype, with the [period](/reference/filtertype_period.md) filtertype, or with the [space](/reference/filtertype_space.md) filtertype if [use_age](/reference/fe_use_age.md) is set to `True`.
::::


```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 3
```

This setting must be one of `seconds`, `minutes`, `hours`, `days`, `weeks`, `months`, or `years`. The values `seconds` and `minutes` are not allowed with the [period](/reference/filtertype_period.md) filtertype and will result in an error condition if used there.

For the [age](/reference/filtertype_age.md) filtertype, or when [use_age](/reference/fe_use_age.md) is set to `True`, unit, [unit_count](/reference/fe_unit_count.md), and optionally, [epoch](/reference/fe_epoch.md), are used by Curator to establish the moment in time point of reference with this formula:

```sh
point_of_reference = epoch - ((number of seconds in unit) * unit_count)
```

`units` are calculated as follows:

| Unit | Seconds | Note |
| --- | --- | --- |
| `seconds` | `1` | One second |
| `minutes` | `60` | Calculated as 60 seconds |
| `hours` | `3600` | Calculated as 60 minutes (60*60) |
| `days` | `86400` | Calculated as 24 hours (24*60*60) |
| `weeks` | `604800` | Calculated as 7 days (7*24*60*60) |
| `months` | `2592000` | Calculated as 30 days (30*24*60*60) |
| `years` | `31536000` | Calculated as 365 days (365*24*60*60) |

If [epoch](/reference/fe_epoch.md) is unset, the current time is used. It is possible to set a point of reference in the future by using a negative value for [unit_count](/reference/fe_unit_count.md).

This setting must be set by the user or an exception will be raised, and execution will halt.

::::{tip}
See the [age filter documentation](/reference/filtertype_age.md) for more information about time calculation.
::::


