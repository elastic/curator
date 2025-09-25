---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/snapshot.html
navigation_title: Snapshot
---

# Snapshot action [snapshot]

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: ...
  # Leaving name blank will result in the default 'curator-%Y%m%d%H%M%S'
  name:
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action will snapshot indices to the indicated [repository](/reference/option_repository.md), with a name, or name pattern, as identified by [name](/reference/option_name.md).

The other options are usually okay to leave at the defaults, but feel free to read about them and change them accordingly.

## Required settings [_required_settings_12]

* [repository](/reference/option_repository.md)


## Optional settings [_optional_settings_17]

* [search_pattern](/reference/option_search_pattern.md)
* [name](/reference/option_name.md)
* [ignore_unavailable](/reference/option_ignore.md)
* [include_global_state](/reference/option_include_gs.md)
* [partial](/reference/option_partial.md)
* [wait_for_completion](/reference/option_wfc.md)
* [max_wait](/reference/option_max_wait.md)
* [wait_interval](/reference/option_wait_interval.md)
* [skip_repo_fs_check](/reference/option_skip_fsck.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_snapshot.md).
::::



