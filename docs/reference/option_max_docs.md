---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_max_docs.html
---

# max_docs [option_max_docs]

```yaml
action: rollover
description: >-
  Rollover the index associated with alias 'aliasname', which should be in the
  form of prefix-000001 (or similar), or prefix-yyyy.MM.DD-1.
options:
  name: aliasname
  conditions:
    max_docs: 1000000
```

::::{note}
At least one of [max_age](/reference/option_max_age.md), max_docs, [max_size](/reference/option_max_size.md) or any combinations of the three are required as `conditions:` for the [Rollover](/reference/rollover.md) action.
::::


The maximum number of documents allowed in an index before triggering a rollover.  It *must* be nested under `conditions:` There is no default value. If this condition is specified, it must have a value, or Curator will generate an error.

