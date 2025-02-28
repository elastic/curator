---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/close.html
---

# Close [close]

```yaml
action: close
description: "Close selected indices"
options:
  delete_aliases: false
  skip_flush: false
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action closes the selected indices, and optionally deletes associated aliases beforehand.

## Optional settings [_optional_settings_3]

* [search_pattern](/reference/option_search_pattern.md)
* [delete_aliases](/reference/option_delete_aliases.md)
* [skip_flush](/reference/option_skip_flush.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_close.md).
::::



