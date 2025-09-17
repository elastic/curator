---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_restore.html
navigation_title: restore
---

# restore action examples [ex_restore]

```yaml
---
# Remember, leave a key empty if there is no value.  None will be a string,
# not a Python "NoneType"
#
# Also remember that all examples have 'disable_action' set to True.  If you
# want to use this action as a template, be sure to set this to False after
# copying it.
actions:
  1:
    action: restore
    description: >-
      Restore all indices in the most recent curator-* snapshot with state
      SUCCESS.  Wait for the restore to complete before continuing.  Do not skip
      the repository filesystem access check.  Use the other options to define
      the index/shard settings for the restore.
    options:
      repository:
      # If name is blank, the most recent snapshot by age will be selected
      name:
      # If indices is blank, all indices in the snapshot will be restored
      indices:
      include_aliases: False
      ignore_unavailable: False
      include_global_state: False
      partial: False
      rename_pattern:
      rename_replacement:
      extra_settings:
      wait_for_completion: True
      skip_repo_fs_check: True
      disable_action: True
    filters:
    - filtertype: pattern
      kind: prefix
      value: curator-
    - filtertype: state
      state: SUCCESS
```

