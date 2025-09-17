---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_number_of_shards.html
applies_to:
  stack:
  serverless: unavailable
products:
  - id: elasticsearch
---

# number_of_shards [option_number_of_shards]

The number_of_shards option specifies the number of primary shards in the target index created by the shrink action.

::::{note}
`number_of_shards` is used only by the [shrink](/reference/shrink.md) action.
::::

* **Type:** Integer
* **Default value:** 1
* **Required:** Optional

The value of `number_of_shards` determines the number of primary shards in the target index after the shrink operation.

::::{important}
The value of `number_of_shards` must meet the following criteria:
* It must be lower than the number of primary shards in the source index.
* It must be a factor of the number of primary shards in the source index.

For example, a source index with 8 primary shards can be shrunk to 4, 2, or 1, and cannot be shrunk to 3 or 5.
::::
  
```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Set the number of shards to 2.
options:
  shrink_node: DETERMINISTIC
  number_of_shards: 2
filters:
  - filtertype: ...
```

Before you use `number_of_shards`, learn about shards and how shard count and sizing affect performance and scalability. For details, check the following:

* [Clusters, nodes, and shards](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards.md)
* [Size your shards](docs-content://deploy-manage/production-guidance/optimize-performance/size-shards.md)



