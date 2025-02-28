---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_preserve_existing.html
---

# preserve_existing [option_preserve_existing]

```yaml
action: index_settings
description: "Change settings for selected indices"
options:
  index_settings:
    index:
      refresh_interval: 5s
  ignore_unavailable: False
  preserve_existing: False
filters:
- filtertype: ...
```

This setting must be either `True` or `False`.

If `preserve_existing` is set to `True`, and the configuration attempts to push a setting to an index that already has any value for that setting, the existing setting will be preserved, and remain unchanged.

The default value of this setting is `False`

