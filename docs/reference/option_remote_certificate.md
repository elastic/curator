---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_remote_certificate.html
---

# remote_certificate [option_remote_certificate]

This should be a file path to a CA certificate, or left empty.

```yaml
actions:
  1:
    description: "Reindex index1 into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      remote_certificate: /path/to/my/ca.cert
      remote_client_cert: /path/to/my/client.cert
      remote_client_key: /path/to/my/client.key
      request_body:
        source:
          remote:
            host: https://otherhost:9200
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```

::::{note}
This option is only used by the [Reindex action](/reference/reindex.md) when performing a remote reindex operation.
::::


This setting allows the use of a specified CA certificate file to validate the SSL certificate used by Elasticsearch.

There is no default.

