---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_include_aliases.html
---

# include_aliases [option_include_aliases]

::::{note}
This setting is only used by the [restore](/reference/restore.md) action.
::::


```yaml
actions:
  1:
    action: restore
    description: Restore my_index from my_snapshot in my_repository
    options:
      repository: my_repository
      name: my_snapshot
      indices: my_index
      include_aliases: True
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

This setting must be either `True` or `False`.

The value of this setting determines whether Elasticsearch should include index aliases when restoring the snapshot.

The default value of this setting is `False`

