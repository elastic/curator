---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_partial.html
---

# partial [option_partial]

::::{note}
This setting is only used by the [snapshot](/reference/snapshot.md) action.
::::


```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name: ...
  partial: False
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```

This setting must be either `True` or `False`.

The entire snapshot will fail if one or more indices being added to the snapshot do not have all primary shards available. This behavior can be changed by setting partial to `True`.

The default value of this setting is `False`

