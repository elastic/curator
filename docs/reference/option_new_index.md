---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_new_index.html
---

# new_index [option_new_index]

::::{note}
This optional setting is only used by the [rollover](/reference/rollover.md) action.
::::


```yaml
description: >-
  Rollover the index associated with alias 'name'.  Specify new index name using
  date math.
options:
  name: aliasname
  new_index: "<prefix-{now/d}-1>"
  conditions:
    max_age: 1d
  wait_for_active_shards: 1
```

::::{important}
A new index name for rollover should still end with a dash followed by an incrementable number, e.g. `my_new_index-1`, or if using date math, `<prefix-{now/d}-1>` or `<prefix-{now/d}-000001>`
::::


## date_math [_date_math_3]

This setting may be a valid [Elasticsearch date math string](elasticsearch://reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names).

A date math name takes the following form:

```sh
<static_name{date_math_expr{date_format|time_zone}}>
```

|     |     |
| --- | --- |
| `static_name` | is the static text part of the name |
| `date_math_expr` | is a dynamic date math expression that computes the date dynamically |
| `date_format` | is the optional format in which the computed date should be rendered. Defaults to `yyyy.MM.dd`. |
| `time_zone` | is the optional time zone . Defaults to `utc`. |

The following example shows different forms of date math names and the final form they resolve to given the current time is 22rd March 2024 noon utc.

| Expression | Resolves to |
| --- | --- |
| `<logstash-{now/d}>` | `logstash-2024.03.22` |
| `<logstash-{now/M}>` | `logstash-2024.03.01` |
| `<logstash-{now/M{yyyy.MM}}>` | `logstash-2024.03` |
| `<logstash-{now/M-1M{yyyy.MM}}>` | `logstash-2024.02` |
| `<logstash-{now/d{yyyy.MM.dd&#124;+12:00}}>` | `logstash-2024.03.23` |

There is no default value for `new_index`.


