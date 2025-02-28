---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/open.html
---

# Open [open]

```yaml
action: open
description: "open selected indices"
options:
  continue_if_exception: False
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action opens the selected indices.

## Optional settings [_optional_settings_11]

* [search_pattern](/reference/option_search_pattern.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_open.md).
::::



