---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_retry_interval.html
---

# retry_interval [option_retry_interval]

::::{note}
This setting is only used by the [delete snapshots action](/reference/delete_snapshots.md).
::::


```yaml
action: delete_snapshots
description: "Delete selected snapshots from 'repository'"
options:
  repository: ...
  retry_interval: 120
  retry_count: 3
filters:
- filtertype: ...
```

The value of this setting is the number of seconds to delay between retries.

The default for this setting is `120`.

