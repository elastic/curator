---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_migration_prefix.html
---

# migration_prefix [option_migration_prefix]

::::{note}
This setting is used by the [reindex](/reference/reindex.md) action.
::::


If the destination index is set to `MIGRATION`, Curator will reindex all selected indices one by one until they have all been reindexed.  By configuring `migration_prefix`, a value can prepend each of those index names.  For example, if I were reindexing `index1`, `index2`, and `index3`, and `migration_prefix` were set to `new-`, then the reindexed indices would be named `new-index1`, `new-index2`, and `new-index3`:

```yaml
actions:
  1:
    description: >-
      Reindex index1, index2, and index3 with a prefix of new-, resulting in
      indices named new-index1, new-index2, and new-index3
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      migration_prefix: new-
      request_body:
        source:
          index: ["index1", "index2", "index3"]
        dest:
          index: MIGRATION
    filters:
    - filtertype: none
```

`migration_prefix` can be used in conjunction with [*migration_suffix*](/reference/option_migration_suffix.md)

