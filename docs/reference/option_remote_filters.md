---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_remote_filters.html
---

# remote_filters [option_remote_filters]

::::{note}
This option is only used by the [Reindex action](/reference/reindex.md) when performing a remote reindex operation.
::::


This is an array of [filters](/reference/filters.md), exactly as with regular index selection:

```yaml
actions:
  1:
    description: "Reindex matching indices into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          remote:
            host: https://otherhost:9200
          index: REINDEX_SELECTION
        dest:
          index: index2
      remote_filters:
      - filtertype: *first*
        setting1: ...
        ...
        settingN: ...
      - filtertype: *second*
        setting1: ...
        ...
        settingN: ...
      - filtertype: *third*
    filters:
    - filtertype: none
```

This feature will only work when the `source` `index` is set to `REINDEX_SELECTION`.  It will select *remote* indices matching the filters provided, and reindex them to the *local* cluster as the name provided in the `dest` `index`.  In this example, that is `index2`.

