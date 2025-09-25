---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/reindex.html
navigation_title: Reindex
---

# Reindex action [reindex]

```yaml
actions:
  1:
    description: "Reindex index1 into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```

There are many options for the reindex option.  The best place to start is in the [request_body documentation](/reference/option_request_body.md) to see how to configure this action.  All other options are as follows.

## Required settings [_required_settings_7]

* [request_body](/reference/option_request_body.md)

## Optional settings [_optional_settings_12]

* [refresh](/reference/option_refresh.md)
* [remote_certificate](/reference/option_remote_certificate.md)
* [remote_client_cert](/reference/option_remote_client_cert.md)
* [remote_client_key](/reference/option_remote_client_key.md)
* [remote_filters](/reference/option_remote_filters.md)
* [remote_url_prefix](/reference/option_remote_url_prefix.md)
* [request_body](/reference/option_request_body.md)
* [requests_per_second](/reference/option_requests_per_second.md)
* [slices](/reference/option_slices.md)
* [timeout](/reference/option_timeout.md)
* [wait_for_active_shards](/reference/option_wait_for_active_shards.md)
* [wait_for_completion](/reference/option_wfc.md)
* [max_wait](/reference/option_max_wait.md)
* [wait_interval](/reference/option_wait_interval.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)
* [migration_prefix](/reference/option_migration_prefix.md)
* [migration_suffix](/reference/option_migration_suffix.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_reindex.md).
::::

## Compatibility [_compatibility]

Generally speaking, the Curator should be able to perform a remote reindex from any version of Elasticsearch, 1.4 and newer. Strictly speaking, the Reindex API in Elasticsearch *is* able to reindex from older clusters, but Curator cannot be used to facilitate this due to Curator’s dependency on changes released in 1.4.
