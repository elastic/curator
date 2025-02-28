---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_closed.html
---

# closed [filtertype_closed]

```yaml
- filtertype: closed
  exclude: True
```

This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices which are closed.  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

## Optional settings [_optional_settings_21]

* [exclude](/reference/fe_exclude.md) (default is `True`)


