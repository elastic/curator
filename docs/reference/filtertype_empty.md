---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_empty.html
---

# empty [filtertype_empty]

```yaml
- filtertype: empty
  exclude: False
```

This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices which do not contain any documents. Indices that are closed are automatically removed from consideration. They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

By default the indices matched by the empty filter will be excluded since the exclude setting defaults to True. To include matching indices rather than exclude, set the exclude setting to False.

## Optional settings [_optional_settings_23]

* [exclude](/reference/fe_exclude.md) (default is `True`)


