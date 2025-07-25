---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_remote_url_prefix.html
---

# remote_url_prefix [option_remote_url_prefix]

::::{note}
This option is only used by the [Reindex action](/reference/reindex.md) when performing a remote reindex operation.
::::


This should be a single value or left empty.

```yaml
actions:
  1:
    description: "Reindex index1 into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      remote_url_prefix: my_prefix
      request_body:
        source:
          remote:
            host: <OTHER_HOST_URL>:9200
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```

In some cases you may be obliged to connect to a remote Elasticsearch cluster through a proxy of some kind. There may be a URL prefix before the API URI items, e.g. [http://example.com/elasticsearch/](http://example.com/elasticsearch/) as opposed to [http://localhost:9200](http://localhost:9200). In such a case, set the `remote_url_prefix` to the appropriate value, *elasticsearch* in this example.

The default is an empty string.

