---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_ignore.html
---

# ignore_unavailable [option_ignore]

::::{note}
This setting is used by the [snapshot](/reference/snapshot.md), [restore](/reference/restore.md), and [index_settings](/reference/index_settings.md) actions.
::::


This setting must be either `True` or `False`.

The default value of this setting is `False`

## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md_2]

```yaml
actions:
  1:
    action: restore
    description: Restore my_index from my_snapshot in my_repository
    options:
      repository: my_repository
      name: my_snapshot
      indices: my_index
      ignore_unavailable: True
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

When the `ignore_unavailable` option is `False` and an index is missing the restore request will fail.


## [snapshot](/reference/snapshot.md) [_snapshot/curator/docs/reference/elasticsearch/elasticsearch-client-curator/snapshot.md]

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name: my_snapshot
  ignore_unavailable: False
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```

When the `ignore_unavailable` option is `False` and an index is missing, the snapshot request will fail.  This is not frequently a concern in Curator, as it should only ever find indices that exist.


## [index_settings](/reference/index_settings.md) [_index_settings/curator/docs/reference/elasticsearch/elasticsearch-client-curator/index_settings.md]

```yaml
action: index_settings
description: "Change settings for selected indices"
options:
  index_settings:
    index:
      refresh_interval: 5s
  ignore_unavailable: False
  preserve_existing: False
filters:
- filtertype: ...
```

When the `ignore_unavailable` option is `False` and an index is missing, or if the request is to apply a [static](docs-content:///deploy-manage/stack-settings.md#static-dynamic) setting and the index is opened, the index setting request will fail. The `ignore_unavailable` option allows these indices to be skipped, when set to `True`.

::::{note}
[Dynamic](docs-content:///deploy-manage/stack-settings.md#static-dynamic) index settings can be applied to either open or closed indices.
::::



