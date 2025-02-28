---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_skip_fsck.html
---

# skip_repo_fs_check [option_skip_fsck]

::::{note}
This setting is used by the [snapshot](/reference/snapshot.md) and [restore](/reference/restore.md) actions.
::::


This setting must be either `True` or `False`.

The default value of this setting is `False`

## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md_7]

Each master and data node in the cluster *must* have write access to the shared filesystem used by the repository for a snapshot to be 100% valid. For the purposes of a [restore](/reference/restore.md), this is a lightweight attempt to ensure that all nodes are *still* actively able to write to the repository, in hopes that snapshots were from all nodes.  It is not otherwise necessary for the purposes of a restore.

Some filesystems may take longer to respond to a check, which results in a false positive for the filesystem access check. For these cases, it is desirable to bypass this verification step, by setting this to `True.`

```yaml
actions:
  1:
    action: restore
    description: Restore my_index from my_snapshot in my_repository
    options:
      repository: my_repository
      name: my_snapshot
      indices: my_index
      skip_repo_fs_check: False
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```


## [snapshot](/reference/snapshot.md) [_snapshot/curator/docs/reference/elasticsearch/elasticsearch-client-curator/snapshot.md_6]

Each master and data node in the cluster *must* have write access to the shared filesystem used by the repository for a snapshot to be 100% valid.

Some filesystems may take longer to respond to a check, which results in a false positive for the filesystem access check. For these cases, it is desirable to bypass this verification step, by setting this to `True.`

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name: my_snapshot
  skip_repo_fs_check: False
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```


