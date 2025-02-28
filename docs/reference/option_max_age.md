---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_max_age.html
---

# max_age [option_max_age]

```yaml
action: rollover
description: >-
  Rollover the index associated with alias 'aliasname', which should be in the
  form of prefix-000001 (or similar), or prefix-yyyy.MM.DD-1.
options:
  name: aliasname
  conditions:
    max_age: 1d
```

::::{note}
At least one of max_age, [max_docs](/reference/option_max_docs.md), [max_size](/reference/option_max_size.md) or any combinations of the three are required as `conditions:` for the [Rollover](/reference/rollover.md) action.
::::


The maximum age that is allowed before triggering a rollover. It *must* be nested under `conditions:` There is no default value.  If this condition is specified, it must have a value, or Curator will generate an error.

Ages such as `1d` for one day, or `30s` for 30 seconds can be used.

