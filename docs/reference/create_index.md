---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/create_index.html
---

# Create Index [create_index]

```yaml
action: create_index
description: "Create index as named"
options:
  name: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action creates the named index.  There are multiple different ways to configure how the name is represented.

## Manual naming [_manual_naming]

```yaml
action: create_index
description: "Create index as named"
options:
  name: myindex
  # ...
```

In this case, what you see is what you get. An index named `myindex` will be created


## Python strftime [_python_strftime]

```yaml
action: create_index
description: "Create index as named"
options:
  name: 'myindex-%Y.%m'
  # ...
```

For the `create_index` action, the [name](/reference/option_name.md) option can contain Python strftime strings.  The method for doing so is described in detail, including which strftime strings are acceptable, in the documentation for the [name](/reference/option_name.md) option.


## Date Math [_date_math]

```yaml
action: create_index
description: "Create index as named"
options:
  name: '<logstash-{now/d+1d}>'
  # ...
```

For the `create_index` action, the [name](/reference/option_name.md) option can be in Elasticsearch [date math](elasticsearch://reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) format.  This allows index names containing dates to use deterministic math to set a date name in the past or the future.

For example, if today’s date were 2017-03-27, the name `<logstash-{now/d}>` will create an index named `logstash-2017.03.27`. If you wanted to create *tomorrow’s* index, you would use the name `<logstash-{now/d+1d}>`, which adds 1 day.  This pattern creates an index named `logstash-2017.03.28`.  For many more configuration options, read the Elasticsearch [date math](elasticsearch://reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) documentation.


## Extra Settings [_extra_settings]

The [extra_settings](/reference/option_extra_settings.md) option allows the addition of extra settings, such as index settings and mappings.  An example of how these settings can be used to create an index might be:

```yaml
action: create_index
description: "Create index as named"
options:
  name: myindex
  # ...
  extra_settings:
    settings:
      number_of_shards: 1
      number_of_replicas: 0
    mappings:
      type1:
        properties:
          field1:
            type: string
            index: not_analyzed
```


## Required settings [_required_settings_4]

* [name](/reference/option_name.md)


## Optional settings [_optional_settings_6]

* [extra_settings](/reference/option_extra_settings.md) No default value.  You can add any acceptable index settings and mappings as nested YAML.  See the [Elasticsearch Create Index API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) for more information.
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)

::::{tip}
See an example of this action in an [actionfile](/reference/actionfile.md) [here](/reference/ex_create_index.md).
::::



