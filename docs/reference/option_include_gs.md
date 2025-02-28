---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_include_gs.html
---

# include_global_state [option_include_gs]

::::{note}
This setting is used by the [snapshot](/reference/snapshot.md) and [restore](/reference/restore.md) actions.
::::


This setting must be either `True` or `False`.

The value of this setting determines whether Elasticsearch should include the global cluster state with the snapshot or restore.

When performing a [snapshot](/reference/snapshot.md), the default value of this setting is `True`.

When performing a [restore](/reference/restore.md), the default value of this setting is `False`.

## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md_3]

```yaml
actions:
  1:
    action: restore
    description: Restore my_index from my_snapshot in my_repository
    options:
      repository: my_repository
      name: my_snapshot
      indices: my_index
      include_global_state: False
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```


## [snapshot](/reference/snapshot.md) [_snapshot/curator/docs/reference/elasticsearch/elasticsearch-client-curator/snapshot.md_2]

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name: my_snapshot
  include_global_state: True
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```


