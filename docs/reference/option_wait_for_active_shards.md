---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_wait_for_active_shards.html
---

# wait_for_active_shards [option_wait_for_active_shards]

::::{note}
This setting is used by the [Reindex](/reference/reindex.md), [Rollover](/reference/rollover.md), and [Shrink](/reference/shrink.md) actions.  Each use it similarly.
::::


This setting determines the number of shard copies that must be active before the client returns. The default value is 1, which implies only the primary shards.

Set to `all` for all shard copies, otherwise set to any non-negative value less than or equal to the total number of copies for the shard (number of replicas + 1)

Read [the Elasticsearch documentation](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/docs-index_.md#index-wait-for-active-shards) for more information.

## Reindex [_reindex]

```yaml
actions:
  1:
    description: "Reindex index1,index2,index3 into new_index"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      wait_for_active_shards: 2
      request_body:
        source:
          index: ['index1', 'index2', 'index3']
        dest:
          index: new_index
    filters:
    - filtertype: none
```


## Rollover [_rollover]

```yaml
action: rollover
description: >-
  Rollover the index associated with alias 'name', which should be in the
  form of prefix-000001 (or similar), or prefix-yyyy.MM.DD-1.
options:
  name: aliasname
  conditions:
    max_age: 1d
    max_docs: 1000000
  wait_for_active_shards: 1
  extra_settings:
    index.number_of_shards: 3
    index.number_of_replicas: 1
  timeout_override:
  continue_if_exception: False
  disable_action: False
```


## Shrink [_shrink]

```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Prepend target index names with 'foo-' and append a suffix of '-shrink'
options:
  shrink_node: DETERMINISTIC
  wait_for_active_shards: all
filters:
  - filtertype: ...
```


