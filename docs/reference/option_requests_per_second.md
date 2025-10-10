---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_requests_per_second.html
---

# requests_per_second [option_requests_per_second]

::::{note}
This option is only used by the [Reindex action](/reference/reindex.md)
::::


```yaml
actions:
  1:
    description: "Reindex index1 into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      requests_per_second: -1
      request_body:
        source:
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```

`requests_per_second` can be set to any positive decimal number (1.4, 6, 1000, etc) and throttles the number of requests per second that the reindex issues or it can be set to `-1` to disable throttling.

The default value for this is option is `-1`.

