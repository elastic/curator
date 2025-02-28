---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_key.html
---

# key [option_key]

::::{note}
This setting is required when using the [allocation action](/reference/allocation.md).
::::


```yaml
action: allocation
description: "Apply shard allocation filtering rules to the specified indices"
options:
  key: ...
  value: ...
  allocation_type: ...
filters:
- filtertype: ...
```

The value of this setting should correspond to a node setting on one or more nodes in your cluster.

For example, you might have set

```sh
node.tag: myvalue
```

in your `elasticsearch.yml` file for one or more of your nodes.  To match allocation in this case, set key to `tag`.

These special attributes are also supported:

| attribute | description |
| --- | --- |
| `_name` | Match nodes by node name |
| `_host_ip` | Match nodes by host IP address (IP associated with hostname) |
| `_publish_ip` | Match nodes by publish IP address |
| `_ip` | Match either `_host_ip` or `_publish_ip` |
| `_host` | Match nodes by hostname |

There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

