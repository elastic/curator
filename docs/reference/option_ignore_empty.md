---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_ignore_empty.html
---

# ignore_empty_list [option_ignore_empty]

This setting must be either `True` or `False`.

```yaml
action: delete_indices
description: "Delete selected indices"
options:
  ignore_empty_list: True
filters:
- filtertype: ...
```

Depending on your indices, and how you’ve filtered them, an empty list may be presented to the action.  This results in an error condition.

When the ignore_empty_list option is set to `True`, the action will exit with an INFO level log message indicating such.  If ignore_empty_list is set to `False`, an ERROR level message will be logged, and Curator will exit with code 1.

The default value of this setting is `False`

