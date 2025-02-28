---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_extra_settings.html
---

# extra_settings [option_extra_settings]

This setting should be nested YAML.  The values beneath `extra_settings` will be used by whichever action uses the option.

## [alias](/reference/alias.md) [_alias/curator/docs/reference/elasticsearch/elasticsearch-client-curator/alias.md]

```yaml
action: alias
description: "Add/Remove selected indices to or from the specified alias"
options:
  name: alias_name
  extra_settings:
    filter:
      term:
        user: kimchy
add:
  filters:
  - filtertype: ...
remove:
  filters:
  - filtertype: ...
```


## [create_index](/reference/create_index.md) [_create_index/curator/docs/reference/elasticsearch/elasticsearch-client-curator/create_index.md]

```yaml
action: create_index
description: "Create index as named"
options:
  name: myindex
  # ...
  extra_settings:
    settings:
      number_of_shards: 1
      number_of_replicas: 0
    mappings:
      type1:
        properties:
          field1:
            type: string
            index: not_analyzed
```


## [restore](/reference/restore.md) [_restore/curator/docs/reference/elasticsearch/elasticsearch-client-curator/restore.md]

See the [official Elasticsearch Documentation](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/snapshots-restore-snapshot.md).

```yaml
actions:
  1:
    action: restore
    description: >-
      Restore all indices in the most recent snapshot with state SUCCESS.  Wait
      for the restore to complete before continuing.  Do not skip the repository
      filesystem access check.  Use the other options to define the index/shard
      settings for the restore.
    options:
      repository:
      # If name is blank, the most recent snapshot by age will be selected
      name:
      # If indices is blank, all indices in the snapshot will be restored
      indices:
      extra_settings:
        index_settings:
          number_of_replicas: 0
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```


## [rollover](/reference/rollover.md) [_rollover/curator/docs/reference/elasticsearch/elasticsearch-client-curator/rollover.md]

```yaml
action: rollover
description: >-
  Rollover the index associated with alias 'name', which should be in the
  form of prefix-000001 (or similar), or prefix-yyyy.MM.DD-1.
options:
  name: aliasname
  conditions:
    max_age: 1d
    max_docs: 1000000
  extra_settings:
    index.number_of_shards: 3
    index.number_of_replicas: 1
  timeout_override:
  continue_if_exception: False
  disable_action: False
```


## [shrink](/reference/shrink.md) [_shrink/curator/docs/reference/elasticsearch/elasticsearch-client-curator/shrink.md]

::::{note}
[Only `settings` and `aliases` are acceptable](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink) when used in [shrink](/reference/shrink.md).
::::


```yaml
action: shrink
description: >-
  Shrink selected indices on the node with the most available space.
  Delete source index after successful shrink, then reroute the shrunk
  index with the provided parameters.
options:
  shrink_node: DETERMINISTIC
  extra_settings:
    settings:
      index.codec: best_compression
    aliases:
      my_alias: {}
filters:
  - filtertype: ...
```

There is no default value.


