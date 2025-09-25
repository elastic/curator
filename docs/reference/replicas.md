---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/replicas.html
---

# Replicas [replicas]

```yaml
action: replicas
description: >- Set the number of replicas per shard for selected
    indices to 'count'
options:
  count: ...
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action will set the number of replicas per shard to the value of [count](/reference/option_count.md).

You can optionally set `wait_for_completion` to `True` to have Curator wait for the replication operation to complete before continuing:

```yaml
action: replicas
description: >- Set the number of replicas per shard for selected
    indices to 'count'
options:
  count: ...
  wait_for_completion: True
  max_wait: 600
  wait_interval: 10
filters:
- filtertype: ...
```

This configuration will wait for a maximum of 600 seconds for all index replicas to be complete before giving up.  A `max_wait` value of `-1` will wait indefinitely.  Curator will poll for completion at `10` second intervals, as defined by `wait_interval`.

## Required settings [_required_settings_8]

* [count](/reference/option_count.md)


## Optional settings [_optional_settings_13]

* [search_pattern](/reference/option_search_pattern.md)
* [wait_for_completion](/reference/option_wfc.md)
* [max_wait](/reference/option_max_wait.md)
* [wait_interval](/reference/option_wait_interval.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_replicas.md).
::::



