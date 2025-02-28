---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_search_pattern.html
---

# search_pattern [option_search_pattern]

::::{note}
This setting is only used by the [*Allocation*](/reference/allocation.md), [*Close*](/reference/close.md), [*Cold2Frozen*](/reference/cold2frozen.md), [*Delete Indices*](/reference/delete_indices.md), [*Forcemerge*](/reference/forcemerge.md), [*Index Settings*](/reference/index_settings.md), [*Open*](/reference/open.md), [*Replicas*](/reference/replicas.md), [*Shrink*](/reference/shrink.md), and [*Snapshot*](/reference/snapshot.md) actions.
::::


```yaml
action: delete_indices
description: "Delete selected indices"
options:
  search_pattern: 'index-*'
filters:
- filtertype: ...
```

The value of this option can be a comma-separated list of data streams, indices, and aliases used to limit the request. Supports wildcards (*). To target all data streams and indices, omit this parameter or use * or _all. If using wildcards it is highly recommended to encapsulate the entire search pattern in single quotes, e.g. `search_pattern: 'a*,b*,c*'`

The default value is `_all`.

