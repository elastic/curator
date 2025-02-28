---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_allow_ilm.html
---

# allow_ilm_indices [option_allow_ilm]

This option allows Curator to manage ILM-enabled indices. Exercise caution that ILM policies and Curator actions will not interfere with each other.

::::{important}
Read more about Curator and Index Lifecycle Management [here](/reference/index.md).
::::


```yaml
action: delete_indices
description: "Delete the specified indices"
options:
  allow_ilm_indices: true
filters:
- filtertype: ...
```

The value of this setting must be either `true` or `false`.

The default value for this setting is `false`.

