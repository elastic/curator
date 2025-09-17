---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_state.html
navigation_title: state
---

# state filter element[fe_state]

::::{note}
This setting is only used with the [state](/reference/filtertype_state.md) filtertype.
::::

```yaml
- filtertype: state
  state: SUCCESS
```

The value for this setting must be one of `SUCCESS`, `PARTIAL`, `FAILED`, or `IN_PROGRESS`.  This setting determines what kind of snapshots will be passed.

The default value for this setting is `SUCCESS`.

