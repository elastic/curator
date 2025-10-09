---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_include_hidden.html
---

# include_hidden

This option allows Curator to act on indices with the setting `hidden: true`,
which is common with data_streams.

:::{important}
If data_stream backing indices are matched by the `search_pattern` and/or after the filters, any attempt to delete the active backing index will result in an error code. The only way to delete all of a data_stream is via the data_stream API.
:::

```yml
action: delete_indices
description: "Delete the specified indices"
options:
  include_hidden: true
filters:
- filtertype: ...
```

The value of this setting must be either `true` or `false`.

The default value for this setting is `false`.