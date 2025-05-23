---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_shrink.html
---

# shrink [ex_shrink]

```yaml
---
# Remember, leave a key empty if there is no value.  None will be a string,
# not a Python "NoneType"
#
# Also remember that all examples have 'disable_action' set to True.  If you
# want to use this action as a template, be sure to set this to False after
# copying it.
actions:
  1:
    action: shrink
    description: >-
      Shrink logstash indices older than 21 days on the node with the most
      available space, excluding the node named 'not_this_node'.
      Delete each source index after successful shrink, then reroute the shrunk
      index with the provided parameters.
    options:
      disable_action: True
      ignore_empty_list: True
      shrink_node: DETERMINISTIC
      node_filters:
        permit_masters: False
        exclude_nodes: ['not_this_node']
      number_of_shards: 1
      number_of_replicas: 1
      shrink_prefix:
      shrink_suffix: '-shrink'
      delete_after: True
      post_allocation:
        allocation_type: include
        key: node_tag
        value: cold
      wait_for_active_shards: 1
      extra_settings:
        settings:
          index.codec: best_compression
      wait_for_completion: True
      wait_for_rebalance: True
      wait_interval: 9
      max_wait: -1
    filters:
    - filtertype: pattern
      kind: prefix
      value: logstash-
    - filtertype: age
      source: creation_date
      direction: older
      unit: days
      unit_count: 21
```

