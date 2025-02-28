---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_node_filters.html
---

# node_filters [option_node_filters]

::::{note}
This setting is only used by the [shrink](/reference/shrink.md) action.
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Allow master/data nodes to be potential shrink targets, but exclude
  'named_node' from potential selection.
options:
  shrink_node: DETERMINISTIC
  node_filters:
    permit_masters: True
    exclude_nodes: ['named_node']
filters:
  - filtertype: ...
```

There is no default value for `node_filters`.

The current sub-options are as follows:

## permit_masters [_permit_masters]

This option indicates whether the shrink action can select master eligible nodes when using `DETERMINISTIC` as the value for [shrink_node](/reference/option_shrink_node.md). The default value is `False`. Please note that this will exclude the elected master, as well as other master-eligible nodes.

::::{important}
If you have a small cluster with only master/data nodes, you must set `permit_masters` to `True` in order to select one of those nodes as a potential [shrink_node](/reference/option_shrink_node.md).

::::



## exclude_nodes [_exclude_nodes]

This option provides means to exclude nodes from selection when using `DETERMINISTIC` as the value for [shrink_node](/reference/option_shrink_node.md).  It should be noted that you *can* use a named node for [shrink_node](/reference/option_shrink_node.md) and then exclude it here, and it will prevent a shrink from occurring.


