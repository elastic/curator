---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_state.html
---

# state [filtertype_state]

```yaml
- filtertype: state
  state: SUCCESS
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match snapshots based on the value of [state](/reference/fe_state.md).  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

## Required settings [_required_settings_21]

* [state](/reference/fe_state.md)


## Optional settings [_optional_settings_30]

* [exclude](/reference/fe_exclude.md) (default is `False`)


