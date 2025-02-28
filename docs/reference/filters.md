---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filters.html
---

# Filters [filters]

Filters are the way to select only the indices (or snapshots) you want.

::::{admonition} Filter chaining
:class: note

It is important to note that while filters can be chained, each is linked by an implied logical **AND** operation.  If you want to match from one of several different patterns, as with a logical **OR** operation, you can do so with the [pattern](/reference/filtertype_pattern.md) filtertype using *regex* as the [kind](/reference/fe_kind.md).

This example shows how to select multiple indices based on them beginning with either `alpha-`, `bravo-`, or `charlie-`:

```yaml
  filters:
  - filtertype: pattern
    kind: regex
    value: '^(alpha-|bravo-|charlie-).*$'
```

Explaining all of the different ways in which regular expressions can be used is outside the scope of this document, but hopefully this gives you some idea of how a regular expression pattern can be used when a logical **OR** is desired.

::::


The index filtertypes are:

* [age](/reference/filtertype_age.md)
* [alias](/reference/filtertype_alias.md)
* [allocated](/reference/filtertype_allocated.md)
* [closed](/reference/filtertype_closed.md)
* [count](/reference/filtertype_count.md)
* [empty](/reference/filtertype_empty.md)
* [forcemerged](/reference/filtertype_forcemerged.md)
* [kibana](/reference/filtertype_kibana.md)
* [none](/reference/filtertype_none.md)
* [opened](/reference/filtertype_opened.md)
* [pattern](/reference/filtertype_pattern.md)
* [period](/reference/filtertype_period.md)
* [space](/reference/filtertype_space.md)

The snapshot filtertypes are:

* [age](/reference/filtertype_age.md)
* [count](/reference/filtertype_count.md)
* [none](/reference/filtertype_none.md)
* [pattern](/reference/filtertype_pattern.md)
* [period](/reference/filtertype_period.md)
* [state](/reference/filtertype_state.md)

You can use [environment variables](/reference/envvars.md) in your configuration files.

