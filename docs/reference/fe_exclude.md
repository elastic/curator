---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_exclude.html
---

# exclude [fe_exclude]

::::{note}
This setting is available in *all* filter types.
::::


If `exclude` is `True`, the filter will remove matches from the actionable list. If `exclude` is `False`, then only matches will be kept in the actionable list.

The default value for this setting is different for each filter type.

## Examples [_examples_2]

```yaml
- filtertype: opened
  exclude: True
```

This filter will result in only `closed` indices being in the actionable list.

```yaml
- filtertype: opened
  exclude: False
```

This filter will result in only `open` indices being in the actionable list.


