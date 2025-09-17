---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/alias.html
navigation_title: Alias
---

# Alias action in {{es}} Curator [alias]

```yaml
action: alias
description: "Add/Remove selected indices to or from the specified alias"
options:
  name: alias_name
add:
  filters:
  - filtertype: ...
remove:
  filters:
  - filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action adds and/or removes indices from the alias identified by [name](/reference/option_name.md)

The [filters](/reference/filters.md) under the `add` and `remove` directives define which indices will be added and/or removed.  This is an atomic action, so adds and removes happen instantaneously.

The [extra_settings](/reference/option_extra_settings.md) option allows the addition of extra settings with the `add` directive.  These settings are ignored for `remove`.  An example of how these settings can be used to create a filtered alias might be:

```yaml
action: alias
description: "Add/Remove selected indices to or from the specified alias"
options:
  name: alias_name
  extra_settings:
    filter:
      term:
        user: kimchy
add:
  filters:
  - filtertype: ...
remove:
  filters:
  - filtertype: ...
```

::::{warning}
Before creating a filtered alias, first ensure that the fields already exist in the mapping.
::::


Learn more about adding filtering and routing to aliases in the [Elasticsearch Alias API documentation](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/indices-aliases.md).

## Required settings [_required_settings]

* [name](/reference/option_name.md)


## Optional settings [_optional_settings]

* [warn_if_no_indices](/reference/option_warn_if_no_indices.md)
* [extra_settings](/reference/option_extra_settings.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_alias.md).
::::



