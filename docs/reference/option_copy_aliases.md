---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_copy_aliases.html
---

# copy_aliases [option_copy_aliases]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Reimplement source index aliases on target index after successful shrink.
options:
  shrink_node: DETERMINISTIC
  copy_aliases: True
filters:
  - filtertype: ...
```

The default value of this setting is `False`.  If `True`, after an index has been successfully shrunk, any aliases associated with the source index will be copied to the target index.

