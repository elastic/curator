---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/delete_indices.html
---

# Delete Indices [delete_indices]

```yaml
action: delete_indices
description: "Delete selected indices"
options:
  continue_if_exception: False
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action deletes the selected indices.

In clusters which are overcrowded with indices, or a high number of shards per node, deletes can take a longer time to process.  In such cases, it may be helpful to set a higher timeout than is set in the [configuration file](/reference/configfile.md).  You can override that [request_timeout](/reference/configfile.md#request_timeout) as follows:

```yaml
action: delete_indices
description: "Delete selected indices"
options:
  timeout_override: 300
  continue_if_exception: False
filters:
- filtertype: ...
```

## Optional settings [_optional_settings_7]

* [search_pattern](/reference/option_search_pattern.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_delete_indices.md).
::::



