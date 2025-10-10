---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_migration_suffix.html
---

# migration_suffix [option_migration_suffix]

::::{note}
This setting is used by the [reindex](/reference/reindex.md) action.
::::


If the destination index is set to `MIGRATION`, Curator will reindex all selected indices one by one until they have all been reindexed.  By configuring `migration_suffix`, a value can be appended to each of those index names.  For example, if I were reindexing `index1`, `index2`, and `index3`, and `migration_suffix` were set to `-new`, then the reindexed indices would be named `index1-new`, `index2-new`, and `index3-new`:

```yaml
actions:
  1:
    description: >-
      Reindex index1, index2, and index3 with a suffix of -new, resulting in
      indices named index1-new, index2-new, and index3-new
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      migration_suffix: -new
      request_body:
        source:
          index: ["index1", "index2", "index3"]
        dest:
          index: MIGRATION
    filters:
    - filtertype: none
```

`migration_suffix` can be used in conjunction with [*migration_prefix*](/reference/option_migration_prefix.md)

