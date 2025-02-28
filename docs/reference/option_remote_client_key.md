---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_remote_client_key.html
---

# remote_client_key [option_remote_client_key]

::::{note}
This option is only used by the [Reindex action](/reference/reindex.md) when performing a remote reindex operation.
::::


This should be a file path to a client key (private key), or left empty.

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

Allows the use of a specified SSL client key file to authenticate to Elasticsearch. If using [client_cert](/reference/configfile.md#client_cert) and the file specified does not also contain the key, use `client_key` to specify the file containing the SSL key. The key file must be an unencrypted key in PEM format.

