---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/cluster_routing.html
---

# Cluster Routing [cluster_routing]

```yaml
action: cluster_routing
description: "Apply routing rules to the entire cluster"
options:
  routing_type:
  value: ...
  setting: enable
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action changes the shard routing allocation for the selected indices.

See [Cluster-level shard allocation and routing settings](elasticsearch://reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md) for more information.

You can optionally set `wait_for_completion` to `True` to have Curator wait for the shard routing to complete before continuing:

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

This configuration will wait for a maximum of 300 seconds for shard routing and reallocation to complete before giving up.  A `max_wait` value of `-1` will wait indefinitely.  Curator will poll for completion at `10` second intervals, as defined by `wait_interval`.

## Required settings [_required_settings_3]

* [routing_type](/reference/option_routing_type.md)
* [value](/reference/option_value.md)
* [setting](/reference/option_setting.md) Currently must be set to `enable`.  This setting is a placeholder for potential future expansion.


## Optional settings [_optional_settings_4]

* [wait_for_completion](/reference/option_wfc.md)
* [max_wait](/reference/option_max_wait.md)
* [wait_interval](/reference/option_wait_interval.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_cluster_routing.md).
::::



