---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_count.html
---

# count [filtertype_count]

```yaml
- filtertype: count
  count: 10
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


This [filtertype](/reference/filtertype.md) will iterate over the actionable list of indices *or* snapshots. They are ordered by age, or by alphabet, so as to guarantee that the correct items will remain in, or be removed from the actionable list based on the values of [count](/reference/fe_count.md), [exclude](/reference/fe_exclude.md), and [reverse](/reference/fe_reverse.md).

## Age-based sorting [_age_based_sorting]

For use cases where "like" items are being counted, and their name pattern guarantees date sorting is equal to alphabetical sorting, it is unnecessary to set [use_age](/reference/fe_use_age.md) to `True`, as item names will be sorted in [reverse](/reference/fe_reverse.md) order by default.  This means that the item count will start beginning with the *newest* indices or snapshots, and proceed through to the oldest.

Where this is not the case, the [`use_age`](/reference/fe_use_age.md) setting can be used to ensure that index or snapshot ages are properly considered for sorting:

```yaml
- filtertype: count
  count: 10
  use_age: True
  source: creation_date
```

All of the age-related settings from the [`age`](/reference/filtertype_age.md) filter are supported, and the same restrictions apply with regard to filtering indices vs. snapshots.


## Pattern-based sorting [_pattern_based_sorting]

```yaml
- filtertype: count
  count: 1
  pattern: '^(.*)-\d{6}$'
  reverse: true
```

This particular example will match indices following the basic rollover pattern of `indexname-######`, and keep the highest numbered index for each group.

For example, given indices `a-000001`, `a-000002`, `a-000003` and `b-000006`, and `b-000007`, the indices will would be matched are `a-000003` and `b-000007`. Indices that do not match the regular expression in `pattern` will be automatically excluded.

This is particularly useful with indices created and managed using the [Rollover API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-rollover), as you can select only the active indices with the above example ([`exclude`](/reference/fe_exclude.md) defaults to `False`). Setting [`exclude`](/reference/fe_exclude.md) to `True` with the above example will *remove* the active rollover indices, leaving only those which have been rolled-over.

While this is perhaps most useful for the aforementioned scenario, it can also be used with age-based indices as well.


## Reversing sorting [_reversing_sorting]

Using the default configuration, [`reverse`](/reference/fe_reverse.md) is `True`.  Given These indices:

```sh
index1
index2
index3
index4
index5
```

And this filter:

```yaml
- filtertype: count
  count: 2
```

Indices `index5` and `index4` will be recognized as the `2` *most recent,* and will be removed from the actionable list, leaving `index1`, `index2`, and `index3` to be acted on by the given [action](/reference/actions.md).

Similarly, given these indices:

```sh
index-2017.03.01
index-2017.03.02
index-2017.03.03
index-2017.03.04
index-2017.03.05
```

And this filter:

```yaml
- filtertype: count
  count: 2
  use_age: True
  source: name
  timestring: '%Y.%m.%d'
```

The result will be similar.  Indices `index-2017.03.05` and `index-2017.03.04` will be recognized as the `2` *most recent,* and will be removed from the actionable list, leaving `index-2017.03.01`, `index-2017.03.02`, and `index-2017.03.03` to be acted on by the given [action](/reference/actions.md).

In some cases, you may wish to filter for the most recent indices.  To accomplish this you set [`reverse`](/reference/fe_reverse.md) to `False`:

```yaml
- filtertype: count
  count: 2
  reverse: False
```

This time indices `index1` and `index2` will be the `2` which will be removed from the actionable list, leaving `index3`, `index4`, and `index5` to be acted on by the given [action](/reference/actions.md).

Likewise with the age sorted indices:

```yaml
- filtertype: count
  count: 2
  use_age: True
  source: name
  timestring: '%Y.%m.%d'
  reverse: True
```

Indices `index-2017.03.01` and `index-2017.03.02` will be the `2` which will be removed from the actionable list, leaving `index-2017.03.03`, `index-2017.03.04`, and `index-2017.03.05` to be acted on by the given [action](/reference/actions.md).


## Required settings [_required_settings_16]

* [count](/reference/fe_count.md)


## Optional settings [_optional_settings_22]

* [reverse](/reference/fe_reverse.md)
* [use_age](/reference/fe_use_age.md)
* [pattern](/reference/fe_pattern.md)
* [source](/reference/fe_source.md) (required if `use_age` is `True`)
* [timestring](/reference/fe_timestring.md) (required if `source` is `name`)
* [exclude](/reference/fe_exclude.md) (default is `True`)


## Index-only settings [_index_only_settings]

* [field](/reference/fe_field.md) (required if `source` is `field_stats`)
* [stats_result](/reference/fe_stats_result.md) (only used if `source` is `field_stats`)


