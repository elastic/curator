---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_max_size.html
---

# max_size [option_max_size]

```yaml
action: rollover
description: >-
  Rollover the index associated with alias 'aliasname', which should be in the
  form of prefix-000001 (or similar), or prefix-yyyy.MM.DD-1.
options:
  name: aliasname
  conditions:
    max_size: 5gb
```

::::{note}
At least one of [max_age](/reference/option_max_age.md), [max_docs](/reference/option_max_docs.md), max_size or any combinations of the three are required as `conditions:` for the [Rollover](/reference/rollover.md) action.
::::


The maximum approximate size an index is allowed to be before triggering a rollover.  Sizes must use Elasticsearch approved [human-readable byte units](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/common-options.md). It *must* be nested under `conditions:` There is no default value.  If this condition is specified, it must have a value, or Curator will generate an error.

