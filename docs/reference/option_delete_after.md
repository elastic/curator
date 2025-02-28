---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_delete_after.html
---

# delete_after [option_delete_after]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Delete source index after successful shrink.
options:
  shrink_node: DETERMINISTIC
  delete_after: True
filters:
  - filtertype: ...
```

The default value of this setting is `True`.  After an index has been successfully shrunk, the source index will be deleted or preserved based on the value of this setting.

