---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_remote_client_cert.html
---

# remote_client_cert [option_remote_client_cert]

::::{note}
This option is only used by the [Reindex action](/reference/reindex.md) when performing a remote reindex operation.
::::


This should be a file path to a client certificate (public key), or left empty.

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

Allows the use of a specified SSL client cert file to authenticate to Elasticsearch. The file may contain both an SSL client certificate and an SSL key, in which case [client_key](/reference/configfile.md#client_key) is not used. If specifying `client_cert`, and the file specified does not also contain the key, use [client_key](/reference/configfile.md#client_key) to specify the file containing the SSL key. The file must be in PEM format, and the key part, if used, must be an unencrypted key in PEM format as well.

