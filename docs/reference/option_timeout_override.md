---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_timeout_override.html
---

# timeout_override [option_timeout_override]

::::{note}
This setting is available in all actions.
::::


```yaml
action: forcemerge
description: >-
  Perform a forceMerge on selected indices to 'max_num_segments' per shard
options:
  max_num_segments: 2
  timeout_override: 21600
filters:
- filtertype: ...
```

If `timeout_override` is unset in your configuration, some actions will try to set a sane default value.

The following table shows these default values:

| Action Name | Default `timeout_override` Value |
| --- | --- |
| `close` | 180 |
| `delete_snapshots` | 300 |
| `forcemerge` | 21600 |
| `restore` | 21600 |
| `snapshot` | 21600 |

All other actions have no default value for `timeout_override`.

This setting must be an integer number of seconds, or an error will result.

This setting is particularly useful for the [forceMerge](/reference/forcemerge.md) action, as all other actions have a new polling behavior when using [wait_for_completion](/reference/option_wfc.md) that should reduce or prevent client timeouts.

