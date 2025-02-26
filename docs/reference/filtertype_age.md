---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_age.html
---

# age [filtertype_age]

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices based on their age.  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

## Age calculation [_age_calculation]

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

All calculations are in epoch time, which is the number of seconds elapsed since 1 Jan 1970.  If no [`epoch`](/reference/fe_epoch.md) is specified in the filter, then the current epoch time-which is always UTC-is used as the basis for comparison.

As epoch time is always increasing, lower numbers indicate dates and times in the past.

When age is calculated, [`unit`](/reference/fe_unit.md) is multiplied by [`unit_count`](/reference/fe_unit_count.md) to obtain a total number of seconds to use as a differential.

For example, if the time at execution were 2017-04-07T15:00:00Z (UTC), then the epoch timestamp would be `1491577200`.  If I had an age filter defined like this:

```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 3
```

The time differential would be `3*24*60*60` seconds, which is `259200` seconds. Subtracting this value from `1491577200` gives us `1491318000`, which is 2017-04-04T15:00:00Z (UTC), exactly 3 days in the past.  The `creation_date` of indices or snapshots is compared to this timestamp. If it is `older`, it stays in the actionable list, otherwise it is removed from the actionable list.

::::{admonition} `age` filter vs. `period` filter
:class: important

The time differential means of calculation can lead to frustration.

Setting `unit` to `months`, and `unit_count` to `3` will actually calculate the age as `3*30*24*60*60`, which is `7776000` seconds. This may be a big deal. If the date is 2017-01-01T02:30:00Z, or `1483237800` in epoch time. Subtracting `7776000` seconds makes `1475461800`, which is 2016-10-03T02:30:00Z. If you were to try to match monthly indices, `index-2016.12`, `index-2016.11`, `2016.10`, `2016.09`, etc., then both `index-2016.09` *and* `index-2016.10` will be *older* than the cutoff date.  This may result in unintended behavior.

Another way this can cause issues is with weeks. Weekly indices may start on Sunday or Monday. The age filter’s calculation doesn’t take this into consideration, and merely tests the difference between execution time and the timestamp on the index (from any `source`).

Another means of selecting indices and snapshots is the [period](/reference/filtertype_period.md) filter, which is perhaps a better choice for selecting weeks and months as it compensates for these differences.

::::



## `name`-based ages [_name_based_ages]

Using `name` as the `source` tells Curator to look for a [`timestring`](/reference/fe_timestring.md) within the index or snapshot name, and convert that into an epoch timestamp (epoch implies UTC).

```yaml
 - filtertype: age
   source: name
   direction: older
   timestring: '%Y.%m.%d'
   unit: days
   unit_count: 3
```

::::{admonition} A word about regular expression matching with timestrings
:class: warning

Timestrings are parsed from strftime patterns, like `%Y.%m.%d`, into regular expressions.  For example, `%Y` is 4 digits, so the regular expression for that looks like `\d{{4}}`, and `%m` is 2 digits, so the regular expression is `\d{{2}}`.

What this means is that a simple timestring to match year and month, `%Y.%m` will result in a regular expression like this: `^.*\d{{4}}\.\d{{2}}.*$`.  This pattern will match any 4 digits, followed by a period `.`, followed by 2 digits, occurring anywhere in the index name.  This means it *will* match monthly indices, like `index-2016.12`, as well as daily indices, like `index-2017.04.01`, which may not be the intended behavior.

To compensate for this, when selecting indices matching a subset of another pattern, use a second filter with `exclude` set to `True`

```yaml
- filtertype: pattern
 kind: timestring
 value: '%Y.%m'
- filtertype: pattern
 kind: timestring
 value: '%Y.%m.%d'
 exclude: True
```

This will prevent the `%Y.%m` pattern from matching the `%Y.%m` part of the daily indices.

**This applies whether using `timestring` as a mere pattern match, or as part of date calculations.**

::::



## `creation_date`-based ages [_creation_date_based_ages]

`creation_date` extracts the epoch time of index or snapshot creation.

```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 3
```


## `field_stats`-based ages [_field_stats_based_ages]

::::{note}
`source` can only be `field_stats` when filtering indices.
::::


In Curator 5.3 and older, source `field_stats` uses the [Field Stats API](http://www.elastic.co/guide/en/elasticsearch/reference/5.6/search-field-stats.md) to calculate either the `min_value` or the `max_value` of the [`field`](/reference/fe_field.md) as the [`stats_result`](/reference/fe_stats_result.md), and then use that value for age comparisons.  In 5.4 and above, even though it is still called `field_stats`, it uses an aggregation to calculate the same values, as the `field_stats` API is no longer used in Elasticsearch 6.x and up.

[`field`](/reference/fe_field.md) must be of type `date` in Elasticsearch.

```yaml
 - filtertype: age
   source: field_stats
   direction: older
   unit: days
   unit_count: 3
   field: '@timestamp'
   stats_result: min_value
```


## Required settings [_required_settings_13]

* [source](/reference/fe_source.md)
* [direction](/reference/fe_direction.md)
* [unit](/reference/fe_unit.md)
* [unit_count](/reference/fe_unit_count.md)


## Dependent settings [_dependent_settings]

* [timestring](/reference/fe_timestring.md) (required if `source` is `name`)
* [field](/reference/fe_field.md) (required if `source` is `field_stats`) [Indices only]
* [stats_result](/reference/fe_stats_result.md) (only used if `source` is `field_stats`) [Indices only]


## Optional settings [_optional_settings_18]

* [unit_count_pattern](/reference/fe_unit_count_pattern.md)
* [epoch](/reference/fe_epoch.md)
* [exclude](/reference/fe_exclude.md) (default is `False`)


