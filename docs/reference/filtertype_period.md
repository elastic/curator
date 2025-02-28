---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_period.html
---

# period [filtertype_period]

This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices or snapshots based on whether they fit within the given time range. They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

```yaml
 - filtertype: period
   period_type: relative
   source: name
   range_from: -1
   range_to: -1
   timestring: '%Y.%m.%d'
   unit: weeks
   week_starts_on: sunday
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


## Relative Periods [_relative_periods]

A relative period will be reckoned relative to execution time, unless an [epoch](/reference/fe_epoch.md) timestamp is provided.  Reckoning is truncated to the most recent whole unit, where a [unit](/reference/fe_unit.md) can be one of `hours`, `days`, `weeks`, `months`, or `years`.  For example, if I selected `hours` as my `unit`, and I began execution at 02:35, then the point of reckoning would be 02:00. This is relatively easy with `days`, `months`, and `years`, but slightly more complicated with `weeks`. Some users may wish to reckon weeks by the ISO standard, which starts weeks on Monday. Others may wish to use Sunday as the first day of the week.  Both are acceptable options with the `period` filter. The default behavior for `weeks` is to have Sunday be the start of the week. This can be overridden with [week_starts_on](/reference/fe_week_starts_on.md) as follows:

```yaml
 - filtertype: period
   period_type: relative
   source: name
   range_from: -1
   range_to: -1
   timestring: '%Y.%m.%d'
   unit: weeks
   week_starts_on: monday
```

[range_from](/reference/fe_range_from.md) and [range_to](/reference/fe_range_to.md) are counters of whole [units](/reference/fe_unit.md). A negative number indicates a whole unit in the past, while a positive number indicates a whole unit in the future. A `0` indicates the present unit. With such a timeline mentality, it is relatively easy to create a date range that will meet your needs.

If the time of execution time is **2017-04-03T13:45:23.831**, this table will help you figure out what the previous whole unit, current unit, and next whole unit will be, in ISO8601 format.

| unit | -1 | 0 | +1 |
| --- | --- | --- | --- |
| hours | 2017-04-03T12:00:00 | 2017-04-03T13:00:00 | 2017-04-03T14:00:00 |
| days | 2017-04-02T00:00:00 | 2017-04-03T00:00:00 | 2017-04-04T00:00:00 |
| weeks sun | 2017-03-26T00:00:00 | 2017-04-02T00:00:00 | 2017-04-09T00:00:00 |
| weeks mon | 2017-03-27T00:00:00 | 2017-04-03T00:00:00 | 2017-04-10T00:00:00 |
| months | 2017-03-01T00:00:00 | 2017-04-01T00:00:00 | 2017-05-01T00:00:00 |
| years | 2016-01-01T00:00:00 | 2017-01-01T00:00:00 | 2018-01-01T00:00:00 |

Ranges must be from older dates to newer dates, or smaller numbers (including negative numbers) to larger numbers or Curator will return an exception.

An example `period` filter demonstrating how to select all daily indices by timestring found in the index name from last month might look like this:

```yaml
 - filtertype: period
   period_type: relative
   source: name
   range_from: -1
   range_to: -1
   timestring: '%Y.%m.%d'
   unit: months
```

Having `range_from` and `range_to` both be the same value will mean that only that whole unit will be selected, in this case, a month.

::::{important}
`range_from` and `range_to` are required for the `relative` pattern type.
::::



## Absolute Periods [_absolute_periods]

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

In addition to relative periods, you can define absolute periods.  These are defined as the period between the [`date_from`](/reference/fe_date_from.md) and the [`date_to`](/reference/fe_date_to.md).  For example, if `date_from` and `date_to` are both `2017.01`, and [`unit`](/reference/fe_unit.md) is `months`, all indices with a `name`, `creation_date`, or `stats_result` (depending on the value of [`source`](/reference/fe_source.md)) within the month of January 2017 will match.

The `date_from` is used to establish the beginning of the time period, regardless of whether `date_from_format` is in hours, and the indices you are trying to filter are in weeks or months.  The format and date of `date_from` will simply set the beginning of the time period.

The `date_to`, `date_to_format`, and `unit` work in conjunction to select the end date.  For example, if my `date_to` were `2017.04`, and `date_to_format` is `%Y.%m` to properly parse that date, it would follow that `unit` would be `months`.

::::{important}
The period filter is smart enough to calculate `months` and `years` properly.  **If `unit` is not `months` or `years`,** then your date range will be `unit` seconds more than the beginning of the `date_from` date, minus 1 second, according to this table:

[`units`](/reference/fe_unit.md) are calculated as follows:

| Unit | Seconds | Note |
| --- | --- | --- |
| `seconds` | `1` | One second |
| `minutes` | `60` | Calculated as 60 seconds |
| `hours` | `3600` | Calculated as 60 minutes (60*60) |
| `days` | `86400` | Calculated as 24 hours (24*60*60) |
| `weeks` | `604800` | Calculated as 7 days (7*24*60*60) |
| `months` | `2592000` | Calculated as 30 days (30*24*60*60) |
| `years` | `31536000` | Calculated as 365 days (365*24*60*60) |

::::


Specific date ranges can be captured with up to whole second resolution:

```yaml
 - filtertype: period
   period_type: absolute
   source: name
   timestring: '%Y.%m.%d.%H'
   unit: seconds
   date_from: 2017-01-01T00:00:00
   date_from_format: '%Y-%m-%dT%H:%M:%S'
   date_to: 2017-01-01T12:34:56
   date_to_format: '%Y-%m-%dT%H:%M:%S'
```

This example will capture indices with an hourly timestamp in their name that fit between the `date_from` and `date_to` timestamps.

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


## Required settings [_required_settings_19]

* [source](/reference/fe_source.md)
* [unit](/reference/fe_unit.md)


## Dependent settings [_dependent_settings_2]

* [range_from](/reference/fe_range_from.md)
* [range_to](/reference/fe_range_to.md)
* [date_from](/reference/fe_date_from.md)
* [date_to](/reference/fe_date_to.md)
* [date_from_format](/reference/fe_date_from_format.md)
* [date_to_format](/reference/fe_date_to_format.md)
* [timestring](/reference/fe_timestring.md) (required if `source` is `name`)
* [field](/reference/fe_field.md) (required if `source` is `field_stats`) [Indices only]
* [stats_result](/reference/fe_stats_result.md) (only used if `source` is `field_stats`) [Indices only]
* [intersect](/reference/fe_intersect.md) (optional if `source` is `field_stats`) [Indices only]


## Optional settings [_optional_settings_28]

* [epoch](/reference/fe_epoch.md)
* [exclude](/reference/fe_exclude.md) (default is `False`)
* [week_starts_on](/reference/fe_week_starts_on.md)
