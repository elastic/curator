---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_skip_flush.html
---

# skip_flush [option_skip_flush]

::::{note}
This setting is only used by the [close action](/reference/close.md), and is optional.
::::


```yaml
action: close
description: "Close selected indices"
options:
  skip_flush: False
filters:
- filtertype: ...
```

If `skip_flush` is set to `True`, Curator will not flush indices to disk before closing. This may be useful for closing red indices before restoring.

The default value is `False`.

