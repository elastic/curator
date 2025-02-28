---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_cluster_routing.html
---

# cluster_routing [ex_cluster_routing]

```yaml
---
# Remember, leave a key empty if there is no value.  None will be a string,
# not a Python "NoneType"
#
# Also remember that all examples have 'disable_action' set to True.  If you
# want to use this action as a template, be sure to set this to False after
# copying it.
#
# This action example has a blank spot at action ID 2.  This is to show that
# Curator can disable allocation before one or more actions, and then re-enable
# it afterward.
actions:
  1:
    action: cluster_routing
    description: >-
      Disable shard routing for the entire cluster.
    options:
      routing_type: allocation
      value: none
      setting: enable
      wait_for_completion: True
      disable_action: True
  2:
    action: (any other action details go here)
    ...
  3:
    action: cluster_routing
    description: >-
      Re-enable shard routing for the entire cluster.
    options:
      routing_type: allocation
      value: all
      setting: enable
      wait_for_completion: True
      disable_action: True
```

