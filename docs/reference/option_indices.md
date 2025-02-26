---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_indices.html
---

# indices [option_indices]

::::{note}
This setting is only used by the [restore](/reference/restore.md) action.
::::


## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md_4]

```yaml
actions:
  1:
    action: restore
    description: Restore my_index from my_snapshot in my_repository
    options:
      repository: my_repository
      name: my_snapshot
      indices: my_index
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

This setting must be a list of indices to restore.  Any valid YAML format for lists are acceptable here.  If `indices` is left empty, or unset, all indices in the snapshot will be restored.

The default value of this setting is an empty setting.


