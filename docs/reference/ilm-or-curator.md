---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ilm-or-curator.html
---

# ILM or Curator? [ilm-or-curator]

If ILM provides the functionality to manage your index lifecycle, and you have at least a Basic license, consider using ILM in place of Curator. Many of the Stack components make use of ILM by default.

## Beats [ilm-beats]

::::{note}
All Beats share a similar ILM configuration. Filebeats is used as a reference here.
::::


Starting with version 7.0, Filebeat uses index lifecycle management by default when it connects to a cluster that supports lifecycle management. Filebeat loads the default policy automatically and applies it to any indices created by Filebeat.

You can view and edit the policy in the Index lifecycle policies UI in Kibana. For more information about working with the UI, see [Index lifecyle policies](docs-content://manage-data/lifecycle/index-lifecycle-management.md).

Read more about Filebeat and ILM in [](beats://reference/filebeat/ilm.md).


## Logstash [ilm-logstash]

::::{note}
The Index Lifecycle Management feature requires version 9.3.1 or higher of the `logstash-output-elasticsearch` plugin.
::::


Logstash can use [index lifecycle management](docs-content://manage-data/lifecycle/index-lifecycle-management.md) to automate the management of indices over time.

The use of Index Lifecycle Management is controlled by the `ilm_enabled` setting. By default, this will automatically detect whether the Elasticsearch instance supports ILM, and will use it if it is available. `ilm_enabled` can also be set to `true` or `false` to override the automatic detection, or disable ILM.

Read more about Logstash and ILM in [](logstash://reference/plugins-outputs-elasticsearch.md#plugins-outputs-elasticsearch-ilm).


