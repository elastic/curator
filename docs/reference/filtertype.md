---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype.html
---

# filtertype [filtertype]

Each filter is defined first by a `filtertype`.  Each filtertype has its own settings, or no settings at all.  In a configuration file, filters are defined as follows:

```yaml
- filtertype: *first*
  setting1: ...
  ...
  settingN: ...
- filtertype: *second*
  setting1: ...
  ...
  settingN: ...
- filtertype: *third*
```

The `-` indicates in the YAML that this is an array element.  Each filtertype declaration must be preceded by a `-` for the filters to be read properly.  This is how Curator can chain filters together.  Anywhere filters can be used, multiple can be chained together in this manner.

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

