---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/forcemerge.html
---

# Forcemerge [forcemerge]

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

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action performs a forceMerge on the selected indices, merging them to [max_num_segments](/reference/option_mns.md) per shard.

::::{warning}
A [`forcemerge`](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/indices-forcemerge.md#indices-forcemerge) should never be executed on an index that is actively receiving data.  It should only ever be performed on indices where no more documents are ever anticipated to be added in the future.
::::


You can optionally pause between each merge for [delay](/reference/option_delay.md) seconds to allow the cluster to quiesce:

```yaml
action: forcemerge
description: >-
  Perform a forceMerge on selected indices to 'max_num_segments' per shard
options:
  max_num_segments: 2
  timeout_override: 21600
  delay: 120
filters:
- filtertype: ...
```

## Required settings [_required_settings_6]

* [max_num_segments](/reference/option_mns.md)


## Optional settings [_optional_settings_9]

* [search_pattern](/reference/option_search_pattern.md)
* [delay](/reference/option_delay.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_forcemerge.md).
::::



