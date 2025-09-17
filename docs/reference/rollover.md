---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/rollover.html
navigation_title: Rollover
---

# Rollover action [rollover]

```yaml
action: rollover
description: >-
  Rollover the index associated with alias 'aliasname', which should be in the
  form of prefix-000001 (or similar), or prefix-YYYY.MM.DD-1.
options:
  name: aliasname
  conditions:
    max_age: 1d
    max_docs: 1000000
    max_size: 5gb
```

This action uses the [Elasticsearch Rollover API](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/indices-rollover-index.md) to create a new index, if any of the described conditions are met.

::::{important}
When choosing `conditions`, **any** one of [max_age](/reference/option_max_age.md), [max_docs](/reference/option_max_docs.md), [max_size](/reference/option_max_size.md), **or any combination of the three** may be used. If multiple are used, then the specified condition for any one of them must be matched for the rollover to occur.
::::


::::{warning}
If one or more of the [max_age](/reference/option_max_age.md), [max_docs](/reference/option_max_docs.md), or [max_size](/reference/option_max_size.md) options are present, they must each have a value. Because there are no default values, none of these conditions can be left empty, or Curator will generate an error.
::::


## Extra settings [_extra_settings_3]

The [extra_settings](/reference/option_extra_settings.md) option allows the addition of extra index settings (but not mappings).  An example of how these settings can be used might be:

```yaml
action: rollover
description: >-
  Rollover the index associated with alias 'aliasname', which should be in the
  form of prefix-000001 (or similar), or prefix-YYYY.MM.DD-1.
options:
  name: aliasname
  conditions:
    max_age: 1d
    max_docs: 1000000
  extra_settings:
    index.number_of_shards: 3
    index.number_of_replicas: 1
  timeout_override:
  continue_if_exception: False
  disable_action: False
```


## Required settings [_required_settings_10]

* [name](/reference/option_name.md) The alias name
* [max_age](/reference/option_max_age.md) The maximum age that is allowed before triggering a rollover. This *must* be nested under `conditions:`. There is no default value. If this condition is specified, it must have a value, or Curator will generate an error.
* [max_docs](/reference/option_max_docs.md) The maximum number of documents allowed in an index before triggering a rollover.  This *must* be nested under `conditions:`. There is no default value.  If this condition is specified, it must have a value, or Curator will generate an error.
* [max_size](/reference/option_max_size.md) The maximum size the index can be before a rollover is triggered. This *must* be nested under `conditions:`. There is no default value.  If this condition is specified, it must have a value, or Curator will generate an error.


## Optional settings [_optional_settings_15]

* [extra_settings](/reference/option_extra_settings.md) No default value.  You can add any acceptable index settings (not mappings) as nested YAML.  See the [Elasticsearch Create Index API documentation](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/indices-create-index.md) for more information.
* [new_index](/reference/option_new_index.md) Specify a new index name.
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_rollover.md).
::::
