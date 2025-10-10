---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_disable.html
---

# disable_action [option_disable]

::::{note}
This setting is available in all actions.
::::


```yaml
action: delete_indices
description: "Delete selected indices"
options:
  disable_action: False
filters:
- filtertype: ...
```

If `disable_action` is set to `True`, Curator will ignore the current action. This may be useful for temporarily disabling actions in a large configuration file.

The default value for this setting is `False`

