---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_warn_if_no_indices.html
---

# warn_if_no_indices [option_warn_if_no_indices]

::::{note}
This setting is only used by the [alias](/reference/alias.md) action.
::::


This setting must be either `True` or `False`.

The default value for this setting is `False`.

```yaml
action: alias
description: "Add/Remove selected indices to or from the specified alias"
options:
  name: alias_name
  warn_if_no_indices: False
add:
  filters:
  - filtertype: ...
remove:
  filters:
  - filtertype: ...
```

This setting specifies whether or not the alias action should continue with a warning or return immediately in the event that the filters in the add or remove section result in an empty index list.

::::{admonition} Improper use of this setting can yield undesirable results
:class: warning

**Ideal use case:** For example, you want to add the most recent seven days of time-series indices into a *lastweek* alias, and remove indices older than seven days from this same alias.  If you do not not yet have any indices older than seven days, this will result in an empty index list condition which will prevent the entire alias action from completing successfully. If `warn_if_no_indices` were set to `True`, however, it would avert that potential outcome.

**Potentially undesirable outcome:** A *non-beneficial* case would be where if `warn_if_no_indices` is set to `True`, and a misconfiguration results in indices not being found, and therefore not being disassociated from the alias.  As a result, an alias that should only query one week now references multiple weeks of data. If `warn_if_no_indices` were set to `False`, this scenario would have been averted because the empty list condition would have resulted in an error.

::::


