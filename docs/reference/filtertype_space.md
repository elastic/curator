---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_space.html
---

# space [filtertype_space]

This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices when their cumulative disk consumption is `greater_than` (default) or `less_than` than [disk_space](/reference/fe_disk_space.md) gigabytes.  They are first ordered by age, or by alphabet, so as to guarantee the oldest indices are deleted first. They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

## Deleting Indices By Space [_deleting_indices_by_space]

```yaml
- filtertype: space
  disk_space: 100
```

This [filtertype](/reference/filtertype.md) is for those who want to retain indices based on disk consumption, rather than by a set number of days. There are some important caveats regarding this choice:

* Elasticsearch cannot calculate the size of closed indices. Elasticsearch does not keep tabs on how much disk-space closed indices consume. If you close indices, your space calculations will be inaccurate.
* Indices consume resources just by existing. You could run into performance and/or operational snags in Elasticsearch as the count of indices climbs.
* You need to manually calculate how much space across all nodes. The total you give will be the sum of all space consumed across all nodes in your cluster. If you use shard allocation to put more shards or indices on a single node, it will not affect the total space reported by the cluster, but you may still run out of space on that node.

These are only a few of the caveats. This is still a valid use-case, especially for those running a single-node test box.

For use cases where "like" indices are being counted, and their name pattern guarantees date sorting is equal to alphabetical sorting, it is unnecessary to set [use_age](/reference/fe_use_age.md) to `True`, as index names will be sorted in [reverse](/reference/fe_reverse.md) order by default.  For this case, this means that disk space calculations will start beginning with the *newest* indices, and proceeding through to the oldest.


## Age-based sorting [_age_based_sorting_2]

```yaml
- filtertype: space
  disk_space: 100
  use_age: True
  source: creation_date
```

For use cases where "like" indices are being counted, and their name pattern guarantees date sorting is equal to alphabetical sorting, it is unnecessary to set [use_age](/reference/fe_use_age.md) to `True`, as index names will be sorted in [reverse](/reference/fe_reverse.md) order by default.  For this case, this means that disk space calculations will start beginning with the *newest* indices, and proceeding through to the oldest.

Where this is not the case, the [`use_age`](/reference/fe_use_age.md) setting can be used to ensure that index or snapshot ages are properly considered for sorting:

All of the age-related settings from the [`age`](/reference/filtertype_age.md) filter are supported.


## Reversing sorting [_reversing_sorting_2]

::::{important}
The [`reverse`](/reference/fe_reverse.md) setting is ignored when [`use_age`](/reference/fe_use_age.md) is `True`. When [`use_age`](/reference/fe_use_age.md) is `True`, sorting is *always* from newest to oldest, ensuring that old indices are always selected first.
::::


Using the default configuration, [`reverse`](/reference/fe_reverse.md) is `True`.  Given These indices:

```sh
index1 10g
index2 10g
index3 10g
index4 10g
index5 10g
```

And this filter:

```yaml
- filtertype: space
  disk_space: 21
```

The indices will be sorted alphabetically and iterated over in the indicated order (the value of [`reverse`](/reference/fe_reverse.md)) and the total disk space compared after adding the size of each successive index. In this example, that means that `index5` will be added first, and the running total of consumed disk space will be `10g`. Since `10g` is less than the indicated threshold of `21`, `index5` will be removed from the actionable list.

On the next iteration, the amount of space consumed by `index4` will be added, which brings the running total to `20g`, which is still less than the `21` threshold, so `index4` is also removed from the actionable list.

This process changes when the iteration adds the disk space consumed by `index3`. Now the running total crosses the `21` threshold established by [`disk_space`](/reference/fe_disk_space.md) (the running total is now `30g`).  Even though it is only `1g` in excess of the total, `index3` will remain in the actionable list. The threshold is absolute.

The remaining indices, `index2` and `index1` will also be in excess of the threshold, so they will also remain in the actionable list.

So in this example `index1`, `index2`, and `index3` will be acted on by the [action](/reference/actions.md) for this block.

If you were to run this with [loglevel](/reference/configfile.md#loglevel) set to `DEBUG`, you might see messages like these in the output:

```sh
...Removed from actionable list: index5, summed disk usage is 10GB and disk limit is 21.0GB.
...Removed from actionable list: index4, summed disk usage is 20GB and disk limit is 21.0GB.
...Remains in actionable list: index3, summed disk usage is 30GB and disk limit is 21.0GB.
...Remains in actionable list: index2, summed disk usage is 40GB and disk limit is 21.0GB.
...Remains in actionable list: index1, summed disk usage is 50GB and disk limit is 21.0GB.
```

In some cases, you may wish to filter in the reverse order.  To accomplish this, you set [`reverse`](/reference/fe_reverse.md) to `False`:

```yaml
- filtertype: space
  disk_space: 21
  reverse: False
```

This time indices `index1` and `index2` will be the ones removed from the actionable list, leaving `index3`, `index4`, and `index5` to be acted on by the given [action](/reference/actions.md).

If you were to run this with [loglevel](/reference/configfile.md#loglevel) set to `DEBUG`, you might see messages like these in the output:

```sh
...Removed from actionable list: index1, summed disk usage is 10GB and disk limit is 21.0GB.
...Removed from actionable list: index2, summed disk usage is 20GB and disk limit is 21.0GB.
...Remains in actionable list: index3, summed disk usage is 30GB and disk limit is 21.0GB.
...Remains in actionable list: index4, summed disk usage is 40GB and disk limit is 21.0GB.
...Remains in actionable list: index5, summed disk usage is 50GB and disk limit is 21.0GB.
```


## Required settings [_required_settings_20]

* [disk_space](/reference/fe_disk_space.md)


## Optional settings [_optional_settings_29]

* [reverse](/reference/fe_reverse.md)
* [use_age](/reference/fe_use_age.md)
* [source](/reference/fe_source.md) (required if `use_age` is `True`)
* [timestring](/reference/fe_timestring.md) (required if `source` is `name`)
* [threshold_behavior](/reference/fe_threshold_behavior.md) (default is `greater_than`)
* [field](/reference/fe_field.md) (required if `source` is `field_stats`)
* [stats_result](/reference/fe_stats_result.md) (only used if `source` is `field_stats`)
* [exclude](/reference/fe_exclude.md) (default is `False`)


