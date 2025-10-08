---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_rename_pattern.html
---

# rename_pattern [option_rename_pattern]

::::{note}
This setting is only used by the [restore](/reference/restore.md) action.
::::


::::{admonition} from the Elasticsearch documentation
:class: tip

The rename_pattern and [rename_replacement](/reference/option_rename_replacement.md) options can be also used to rename indices on restore using regular expression that supports referencing the original text as explained [here](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Matcher.md#appendReplacement(java.lang.StringBuffer,%20java.lang.String)).

::::


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
      rename_pattern: 'index(.+)'
      rename_replacement: 'restored_index$1'
      wait_for_completion: True
      max_wait: 3600
      wait_interval: 10
    filters:
    - filtertype: state
      state: SUCCESS
      exclude:
    - filtertype: ...
```

In this configuration, Elasticsearch will capture whatever appears after `index` and put it after `restored_index`.  For example, if I was restoring `index-2017.03.01`, the resulting index would be renamed to `restored_index-2017.03.01`.

Read more about this setting in [Restore a snapshot](/deploy-manage/tools/snapshot-and-restore/restore-snapshot).

There is no default value.

