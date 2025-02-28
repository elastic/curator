---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_opened.html
---

# opened [filtertype_opened]

```yaml
- filtertype: opened
  exclude: True
```

This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices which are opened.  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

## Optional settings [_optional_settings_26]

* [exclude](/reference/fe_exclude.md) (default is `True`)


