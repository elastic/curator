---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_alias.html
---

# alias [filtertype_alias]

```yaml
- filtertype: alias
  aliases: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices based on whether they are associated with the given [aliases](/reference/fe_aliases.md), which can be a single value, or an array.  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

::::{admonition} Matching Indices and Aliases
:class: important

[Indices must be in all aliases to match](https://www.elastic.co/guide/en/elasticsearch/reference/5.5/breaking-changes-5.5.html#breaking_55_rest_changes).

If a list of [aliases](/reference/fe_aliases.md) is provided (instead of only one), indices must appear in *all* listed [aliases](/reference/fe_aliases.md) or a 404 error will result, leading to no indices being matched. In older versions, if the index was associated with even one of the aliases in [aliases](/reference/fe_aliases.md), it would result in a match.

::::


## Required settings [_required_settings_14]

* [aliases](/reference/fe_aliases.md)


## Optional settings [_optional_settings_19]

* [exclude](/reference/fe_exclude.md)


