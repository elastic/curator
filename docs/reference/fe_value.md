---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_value.html
---

# value [fe_value]

::::{note}
This setting is only used with the [pattern](/reference/filtertype_pattern.md) filtertype and is a required setting.  There is a separate [value option](/reference/option_value.md) associated with the [allocation action](/reference/allocation.md), and the [allocated filtertype](/reference/filtertype_allocated.md).
::::


The value of this setting is used by [kind](/reference/fe_kind.md) as follows:

* `prefix`: Search the first part of an index name for the provided value
* `suffix`: Search the last part of an index name for the provided value
* `regex`: Provide your own regular expression, and Curator will find the matches.
* `timestring`: An strftime string to extrapolate and find indices that match. For example, given a `timestring` of `'%Y.%m.%d'`, matching indices would include `logstash-2016.04.01` and `.marvel-2016.04.01`, but not `myindex-2016-04-01`, as the pattern is different.

::::{important}
Whatever you provide for `value` is always going to be a part of a<br> regular expression.  The safest practice is to always encapsulate within single quotes.  For example: `value: '-suffix'`, or `value: 'prefix-'`
::::


There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

::::{tip}
There are context-specific examples using `value` in the [kind](/reference/fe_kind.md) documentation.
::::


