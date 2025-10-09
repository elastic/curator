---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_refresh.html
---

# refresh [option_refresh]

::::{note}
This setting is only used by the [reindex](/reference/reindex.md) action.
::::


```yaml
actions:
  1:
    description: "Reindex index1 into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      refresh: True
      request_body:
        source:
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```

Setting `refresh` to `True` will cause all re-indexed indexes to be refreshed. This differs from the Index API’s refresh parameter which causes just the *shard* that received the new data to be refreshed.

Read more about this setting in the [Reindex API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex).

The default value is `True`.

