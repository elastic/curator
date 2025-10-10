---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_wfc.html
---

# wait_for_completion [option_wfc]

::::{note}
This setting is used by the [allocation](/reference/allocation.md), [cluster_routing](/reference/cluster_routing.md), [reindex](/reference/reindex.md), [replicas](/reference/replicas.md), [restore](/reference/restore.md), and [snapshot](/reference/snapshot.md) actions.
::::


This setting must be either `True` or `False`.

This setting specifies whether or not the request should return immediately or wait for the operation to complete before returning.

## [allocation](/reference/allocation.md) [_allocation/curator/docs/reference/elasticsearch/elasticsearch-client-curator/allocation.md_3]

```yaml
action: allocation
description: "Apply shard allocation filtering rules to the specified indices"
options:
  key: ...
  value: ...
  allocation_type: ...
  wait_for_completion: False
  max_wait: 300
  wait_interval: 10
filters:
- filtertype: ...
```

The default value for the [allocation](/reference/allocation.md) action is `False`.


## [cluster_routing](/reference/cluster_routing.md) [_cluster_routing/curator/docs/reference/elasticsearch/elasticsearch-client-curator/cluster_routing.md_3]

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

The default value for the [cluster_routing](/reference/cluster_routing.md) action is `False`.


## [reindex](/reference/reindex.md) [_reindex/curator/docs/reference/elasticsearch/elasticsearch-client-curator/reindex.md_2]

```yaml
actions:
  1:
    description: "Reindex index1 into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```

The default value for the [reindex](/reference/reindex.md) action is `False`.


## [replicas](/reference/replicas.md) [_replicas/curator/docs/reference/elasticsearch/elasticsearch-client-curator/replicas.md_2]

```yaml
action: replicas
description: >- Set the number of replicas per shard for selected
    indices to 'count'
options:
  count: ...
  wait_for_completion: True
  max_wait: 600
  wait_interval: 10
filters:
- filtertype: ...
```

The default value for the [replicas](/reference/replicas.md) action is `False`.


## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md_8]

```yaml
actions:
  1:
    action: restore
    description: Restore my_index from my_snapshot in my_repository
    options:
      repository: my_repository
      name: my_snapshot
      indices: my_index
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

The default value for the [restore](/reference/restore.md) action is `True`.


## [snapshot](/reference/snapshot.md) [_snapshot/curator/docs/reference/elasticsearch/elasticsearch-client-curator/snapshot.md_7]

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name: my_snapshot
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```

The default value for the [snapshot](/reference/snapshot.md) action is `True`.

::::{tip}
During snapshot initialization, information about all previous snapshots is loaded into the memory, which means that in large repositories it may take several seconds (or even minutes) for this command to return even if the `wait_for_completion` setting is set to `False`.
::::



