---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_value.html
---

# value [option_value]

::::{note}
This setting is optional when using the [allocation action](/reference/allocation.md) and required when using the [cluster_routing action](/reference/cluster_routing.md).
::::


## [allocation](/reference/allocation.md) [_allocation/curator/docs/reference/elasticsearch/elasticsearch-client-curator/allocation.md_2]

For the [allocation action](/reference/allocation.md), the value of this setting should correspond to a node setting on one or more nodes in your cluster

For example, you might have set

```sh
node.tag: myvalue
```

in your `elasticsearch.yml` file for one or more of your nodes.  To match allocation in this case, set value to `myvalue`. Additonally, if you used one of the special attribute names `_ip`, `_name`, `_id`, or `_host` for [key](/reference/option_key.md), value can match the one of the node ip addresses, names, ids, or host names, respectively.

::::{note}
To remove a routing allocation, the value of this setting should be left empty, or the `value` setting not even included as an option.
::::


For example, you might have set

```sh
PUT test/_settings
{
  "index.routing.allocation.exclude.size": "small"
}
```

to keep index `test` from allocating shards on nodes that have `node.tag: small`. To remove this shard routing allocation setting, you might use an action file similar to this:

```yaml
---
  actions:
    1:
      action: allocation
      description: ->
        Unset 'index.routing.allocation.exclude.size' for index 'test' by
        passing an empty value.
      options:
        key: size
        value: ...
        allocation_type: exclude
      filters:
      - filtertype: pattern
        kind: regex
        value: '^test$'
```


## [cluster_routing](/reference/cluster_routing.md) [_cluster_routing/curator/docs/reference/elasticsearch/elasticsearch-client-curator/cluster_routing.md_2]

For the [cluster_routing action](/reference/cluster_routing.md), the acceptable values for this setting depend on the value of [routing_type](/reference/option_routing_type.md).

```yaml
action: cluster_routing
description: "Apply routing rules to the entire cluster"
options:
  routing_type: ...
  value: ...
  setting: enable
  wait_for_completion: True
  max_wait: 300
  wait_interval: 10
```

Acceptable values when [routing_type](/reference/option_routing_type.md) is either `allocation` or `rebalance` are `all`, `primaries`, and  `none` (string, not `NoneType`).

If `routing_type` is `allocation`, this can also be `new_primaries`. If `routing_type` is `rebalance`, then the value can also be `replicas`.

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.


