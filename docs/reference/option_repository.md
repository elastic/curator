---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_repository.html
---

# repository [option_repository]

::::{note}
This setting is only used by the [snapshot](/reference/snapshot.md), and [delete snapshots](/reference/delete_snapshots.md) actions.
::::


There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md_6]

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


## [snapshot](/reference/snapshot.md) [_snapshot/curator/docs/reference/elasticsearch/elasticsearch-client-curator/snapshot.md_5]

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name: my_snapshot
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```


