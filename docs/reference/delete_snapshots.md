---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/delete_snapshots.html
---

# Delete Snapshots [delete_snapshots]

```yaml
action: delete_snapshots
description: "Delete selected snapshots from 'repository'"
options:
  repository: ...
  retry_interval: 120
  retry_count: 3
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action deletes the selected snapshots from the selected [repository](/reference/option_repository.md).  If a snapshot is currently underway, Curator will retry up to [retry_count](/reference/option_retry_count.md) times, with a delay of [retry_interval](/reference/option_retry_interval.md) seconds between retries.

## Required settings [_required_settings_5]

* [repository](/reference/option_repository.md)


## Optional settings [_optional_settings_8]

* [retry_interval](/reference/option_retry_interval.md)
* [retry_count](/reference/option_retry_count.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_delete_snapshots.md).
::::



