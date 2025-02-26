---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_epoch.html
---

# epoch [fe_epoch]

::::{note}
This setting is available in the [age](/reference/filtertype_age.md) filtertype, and any filter which has the [`use_age`](/reference/fe_use_age.md) setting. This setting is strictly optional.
::::


::::{tip}
This setting is not common.  It is most frequently used for testing.
::::


[unit](/reference/fe_unit.md), [unit_count](/reference/fe_unit_count.md), and optionally, epoch, are used by Curator to establish the moment in time point of reference with this formula:

```sh
point_of_reference = epoch - ((number of seconds in unit) * unit_count)
```

If epoch is unset, the current time is used. It is possible to set a point of reference in the future by using a negative value for [unit_count](/reference/fe_unit_count.md).

## Example [_example]

```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 3
   epoch: 1491577200
```

The value for this setting must be an epoch timestamp. In this example, the given epoch time of `1491577200` is 2017-04-04T15:00:00Z (UTC).  This will use 3 days older than that timestamp as the point of reference for age comparisons.


