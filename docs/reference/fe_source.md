---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_source.html
---

# source [fe_source]

The *source* from which to derive the index or snapshot age. Can be one of `name`, `creation_date`, or `field_stats`.

::::{note}
This setting is only used with the [age](/reference/filtertype_age.md) filtertype, or<br> with the [space](/reference/filtertype_space.md) filtertype when [use_age](/reference/fe_use_age.md) is set to `True`.
::::


::::{note}
When using the [age](/reference/filtertype_age.md) filtertype, source requires<br> [direction](/reference/fe_direction.md), [unit](/reference/fe_unit.md), [unit_count](/reference/fe_unit_count.md),<br> and additionally, the optional setting, [epoch](/reference/fe_epoch.md).
::::


## `name`-based ages [_name_based_ages_2]

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



## `creation_date`-based ages [_creation_date_based_ages_2]

`creation_date` extracts the epoch time of index or snapshot creation.

```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 3
```


## `field_stats`-based ages [_field_stats_based_ages_2]

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


