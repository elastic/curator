---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_timeout.html
---

# timeout [option_timeout]

::::{note}
This setting is only used by the [reindex](/reference/reindex.md) action.
::::


The `timeout` is the length in seconds each individual bulk request should wait for shards that are unavailable. The default value is `60`, meaning 60 seconds.

```yaml
actions:
  1:
    description: "Reindex index1,index2,index3 into new_index"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      timeout: 90
      request_body:
        source:
          index: ['index1', 'index2', 'index3']
        dest:
          index: new_index
    filters:
    - filtertype: none
```

