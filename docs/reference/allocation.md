---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/allocation.html
---

# Using the allocation action in {{es}} Curator [allocation]

```yaml
action: allocation
description: "Apply shard allocation filtering rules to the specified indices"
options:
  key: ...
  value: ...
  allocation_type: ...
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action changes the shard routing allocation for the selected indices.

See [http://www.elastic.co/guide/en/elasticsearch/reference/8.15/shard-allocation-filtering.html](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/shard-allocation-filtering.html) for more information.

You can optionally set `wait_for_completion` to `True` to have Curator wait for the shard routing to complete before continuing:

```yaml
action: allocation
description: "Apply shard allocation filtering rules to the specified indices"
options:
  key: ...
  value: ...
  allocation_type: ...
  wait_for_completion: True
  max_wait: 300
  wait_interval: 10
filters:
- filtertype: ...
```

This configuration will wait for a maximum of 300 seconds for shard routing and reallocation to complete before giving up.  A `max_wait` value of `-1` will wait indefinitely.  Curator will poll for completion at `10` second intervals, as defined by `wait_interval`.

## Required settings [_required_settings_2]

* [key](/reference/option_key.md)


## Optional settings [_optional_settings_2]

* [search_pattern](/reference/option_search_pattern.md)
* [allocation_type](/reference/option_allocation_type.md)
* [value](/reference/option_value.md)
* [wait_for_completion](/reference/option_wfc.md)
* [max_wait](/reference/option_max_wait.md)
* [wait_interval](/reference/option_wait_interval.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_allocation.md).
::::



