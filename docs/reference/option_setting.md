---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_setting.html
---

# setting [option_setting]

::::{note}
This setting is only used by the [cluster_routing action](/reference/cluster_routing.md).
::::


```yaml
action: cluster_routing
description: "Apply routing rules to the entire cluster"
options:
  routing_type:
  value: ...
  setting: enable
  wait_for_completion: True
  max_wait: 300
  wait_interval: 10
```

The value of this must be `enable` at present.  It is a placeholder for future expansion.

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

