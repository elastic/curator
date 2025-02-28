---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_direction.html
---

# direction [fe_direction]

::::{note}
This setting is only used with the [age](/reference/filtertype_age.md) filtertype.
::::


```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 3
```

This setting must be either `older` or `younger`.  This setting is used to determine whether indices or snapshots are `older` or `younger` than the reference point in time determined by [unit](/reference/fe_unit.md), [unit_count](/reference/fe_unit_count.md), and optionally, [epoch](/reference/fe_epoch.md).  If `direction` is `older`, then indices (or snapshots) which are *older* than the reference point in time will be matched.  Likewise, if `direction` is `younger`, then indices (or snapshots) which are *younger* than the reference point in time will be matched.

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

