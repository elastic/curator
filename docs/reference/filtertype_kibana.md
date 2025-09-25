---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_kibana.html
---

# kibana [filtertype_kibana]

```yaml
- filtertype: kibana
  exclude: True
```

This [filtertype](/reference/filtertype.md) will remove any index matching the regular expression `^\.kibana.*$` from the list of indices, if present.

This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices matching the regular expression `^\.kibana.*$`. They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

## Optional settings [_optional_settings_25]

* [exclude](/reference/fe_exclude.md) (default is `True`)


