---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_max_wait.html
---

# max_wait [option_max_wait]

::::{note}
This setting is used by the [allocation](/reference/allocation.md), [cluster_routing](/reference/cluster_routing.md), [reindex](/reference/reindex.md), [replicas](/reference/replicas.md), [restore](/reference/restore.md), and [snapshot](/reference/snapshot.md) actions.
::::


This setting must be a positive integer, or `-1`.

This setting specifies how long in seconds to wait to see if the action has completed before giving up.  This option is used in conjunction with [wait_interval](/reference/option_wait_interval.md), which is the number of seconds to wait between checking to see if the given action is complete.

The default value for this setting is `-1`, meaning that Curator will wait indefinitely for the action to complete.

## [allocation](/reference/allocation.md) [_allocation/curator/docs/reference/elasticsearch/elasticsearch-client-curator/allocation.md]

```yaml
action: allocation
description: "Apply shard allocation filtering rules to the specified indices"
options:
  key: ...
  value: ...
  allocation_type: ...
  wait_for_completion: True
  max_wait: 300
  wait_interval: 10
filters:
- filtertype: ...
```


## [cluster_routing](/reference/cluster_routing.md) [_cluster_routing/curator/docs/reference/elasticsearch/elasticsearch-client-curator/cluster_routing.md]

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


## [reindex](/reference/reindex.md) [_reindex/curator/docs/reference/elasticsearch/elasticsearch-client-curator/reindex.md]

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


## [replicas](/reference/replicas.md) [_replicas/curator/docs/reference/elasticsearch/elasticsearch-client-curator/replicas.md]

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


## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md_5]

```yaml
actions:
  1:
    action: restore
    description: Restore my_index from my_snapshot in my_repository
    options:
      repository: my_repository
      name: my_snapshot
      indices: my_index
      include_global_state: False
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```


## [snapshot](/reference/snapshot.md) [_snapshot/curator/docs/reference/elasticsearch/elasticsearch-client-curator/snapshot.md_3]

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name: my_snapshot
  include_global_state: True
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```


