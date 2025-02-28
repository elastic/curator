---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_delete_aliases.html
---

# delete_aliases [option_delete_aliases]

::::{note}
This setting is only used by the [close action](/reference/close.md), and is optional.
::::


```yaml
action: close
description: "Close selected indices"
options:
  delete_aliases: False
filters:
- filtertype: ...
```

The value for this setting determines whether aliases will be deleted from indices before closing.

The default value is `False`.

