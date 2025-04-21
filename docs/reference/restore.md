---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/restore.html
---

# Restore [restore]

```yaml
actions:
  1:
    action: restore
    description: >-
      Restore all indices in the most recent snapshot with state SUCCESS.  Wait
      for the restore to complete before continuing.  Do not skip the repository
      filesystem access check.  Use the other options to define the index/shard
      settings for the restore.
    options:
      repository:
      # If name is blank, the most recent snapshot by age will be selected
      name:
      # If indices is blank, all indices in the snapshot will be restored
      indices:
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action will restore indices from the indicated [repository](/reference/option_repository.md), from the most recent snapshot identified by the applied filters, or the snapshot identified by [name](/reference/option_name.md).

## Renaming indices on restore [_renaming_indices_on_restore]

You can cause indices to be renamed at restore with the [rename_pattern](/reference/option_rename_pattern.md) and [rename_replacement](/reference/option_rename_replacement.md) options:

```yaml
actions:
  1:
    action: restore
    description: >-
      Restore all indices in the most recent snapshot with state SUCCESS.  Wait
      for the restore to complete before continuing.  Do not skip the repository
      filesystem access check.  Use the other options to define the index/shard
      settings for the restore.
    options:
      repository:
      # If name is blank, the most recent snapshot by age will be selected
      name:
      # If indices is blank, all indices in the snapshot will be restored
      indices:
      rename_pattern: 'index(.+)'
      rename_replacement: 'restored_index$1'
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

In this configuration, Elasticsearch will capture whatever appears after `index` and put it after `restored_index`.  For example, if I was restoring `index-2017.03.01`, the resulting index would be renamed to `restored_index-2017.03.01`.


## Extra settings [_extra_settings_2]

The [extra_settings](/reference/option_extra_settings.md) option allows the addition of extra settings, such as index settings.  An example of how these settings can be used to change settings for an index being restored might be:

```yaml
actions:
  1:
    action: restore
    description: >-
      Restore all indices in the most recent snapshot with state SUCCESS.  Wait
      for the restore to complete before continuing.  Do not skip the repository
      filesystem access check.  Use the other options to define the index/shard
      settings for the restore.
    options:
      repository:
      # If name is blank, the most recent snapshot by age will be selected
      name:
      # If indices is blank, all indices in the snapshot will be restored
      indices:
      extra_settings:
        index_settings:
          number_of_replicas: 0
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

In this case, the number of replicas will be applied to the restored indices.

For more information see [Restore a snapshot](docs-content://deploy-manage/tools/snapshot-and-restore/restore-snapshot.md).


## Required settings [_required_settings_9]

* [repository](/reference/option_repository.md)


## Optional settings [_optional_settings_14]

* [name](/reference/option_name.md)
* [include_aliases](/reference/option_include_aliases.md)
* [indices](/reference/option_indices.md)
* [ignore_unavailable](/reference/option_ignore.md)
* [include_global_state](/reference/option_include_gs.md)
* [partial](/reference/option_partial.md)
* [rename_pattern](/reference/option_rename_pattern.md)
* [rename_replacement](/reference/option_rename_replacement.md)
* [extra_settings](/reference/option_extra_settings.md)
* [wait_for_completion](/reference/option_wfc.md)
* [max_wait](/reference/option_max_wait.md)
* [wait_interval](/reference/option_wait_interval.md)
* [skip_repo_fs_check](/reference/option_skip_fsck.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_restore.md).
::::



