---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_name.html
---

# name [option_name]

::::{note}
This setting is used by the [alias](/reference/alias.md), [create_index](/reference/create_index.md) and [snapshot](/reference/snapshot.md), actions.
::::


The value of this setting is the name of the alias, snapshot, or index, depending on which action makes use of `name`.

## date math [_date_math_2]

This setting may be a valid [Elasticsearch date math string](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/api-conventions.md#api-date-math-index-names).

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


## strftime [_strftime]

This setting may alternately contain a valid Python strftime string.  Curator will extract the strftime identifiers and replace them with the corresponding values.

The identifiers that Curator currently recognizes include:

| Unit | Value |
| --- | --- |
| `%Y` | 4 digit year |
| `%y` | 2 digit year |
| `%m` | 2 digit month |
| `%W` | 2 digit week of the year |
| `%d` | 2 digit day of the month |
| `%H` | 2 digit hour of the day, in 24 hour notation |
| `%M` | 2 digit minute of the hour |
| `%S` | 2 digit second of the minute |
| `%j` | 3 digit day of the year |


## [alias](/reference/alias.md) [_alias/curator/docs/reference/elasticsearch/elasticsearch-client-curator/alias.md_2]

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

This option is required by the [alias](/reference/alias.md) action, and has no default setting in that context.


## [create_index](/reference/create_index.md) [_create_index/curator/docs/reference/elasticsearch/elasticsearch-client-curator/create_index.md_2]

For the [create_index](/reference/create_index.md) action, there is no default setting, but you can use strftime:

```yaml
action: create_index
description: "Create index as named"
options:
  name: 'myindex-%Y.%m'
  # ...
```

or use Elasticsearch [date math](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/api-conventions.md#api-date-math-index-names)

```yaml
action: create_index
description: "Create index as named"
options:
  name: '<logstash-{now/d+1d}>'
  # ...
```

to name your indices.  See more in the [create_index](/reference/create_index.md) documenation.


## [snapshot](/reference/snapshot.md) [_snapshot/curator/docs/reference/elasticsearch/elasticsearch-client-curator/snapshot.md_4]

```yaml
action: snapshot
description: >-
  Snapshot selected indices to 'repository' with the snapshot name or name
  pattern in 'name'.  Use all other options as assigned
options:
  repository: my_repository
  name:
  include_global_state: True
  wait_for_completion: True
  max_wait: 3600
  wait_interval: 10
filters:
- filtertype: ...
```

For the [snapshot](/reference/snapshot.md) action, the default value of this setting is `curator-%Y%m%d%H%M%S`


