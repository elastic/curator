---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/index_settings.html
---

# Index Settings [index_settings]

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

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action updates the specified index settings for the selected indices.

::::{important}
While Elasticsearch allows for either dotted notation of index settings, such as

```json
PUT /indexname/_settings
{
  "index.blocks.read_only": true
}
```

or in nested structure, like this:

```json
PUT /indexname/_settings
{
  "index": {
    "blocks": {
      "read_only": true
    }
  }
}
```

In order to appropriately detect static vs. dynamic [index settings](elasticsearch://docs/reference/elasticsearch/index-settings/index.md) and to be able to verify configurational integrity in the YAML file, **Curator does not support using dotted notation.**

::::


## Optional settings [_optional_settings_10]

* [search_pattern](/reference/option_search_pattern.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)
* [ignore_unavailable](/reference/option_ignore.md)
* [preserve_existing](/reference/option_preserve_existing.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_index_settings.md).
::::
