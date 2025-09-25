---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_shrink_node.html
---

# shrink_node [option_shrink_node]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space, excluding
  master nodes and the node named 'not_this_node'
options:
  shrink_node: DETERMINISTIC
  node_filters:
    permit_masters: False
    exclude_nodes: ['not_this_node']
  shrink_suffix: '-shrink'
filters:
  - filtertype: ...
```

This setting is required.  There is no default value.

The value of this setting must be the valid name of a node in your Elasticsearch cluster, or `DETERMINISTIC`.  If the value is `DETERMINISTIC`, Curator will automatically select the data node with the most available free space and make that the target node. Curator will repeat this process for each successive index when the value is `DETERMINISTIC`.

If [node_filters](/reference/option_node_filters.md), such as `exclude_nodes` are defined, those nodes will not be considered as potential target nodes.

