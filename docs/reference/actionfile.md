---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/actionfile.html
---

# Action File [actionfile]

::::{note}
You can use [environment variables](/reference/envvars.md) in your configuration files.
::::


An action file has the following structure:

```sh
---
# Remember, leave a key empty if there is no value.  None will be a string,
# not a Python "NoneType"
#
# Also remember that all examples have 'disable_action' set to True.  If you
# want to use this action as a template, be sure to set this to False after
# copying it.
actions:
  1:
    action: ACTION1
    description: OPTIONAL DESCRIPTION
    options:
      option1: value1
      ...
      optionN: valueN
      continue_if_exception: False
      disable_action: True
    filters:
    - filtertype: *first*
      filter_element1: value1
      ...
      filter_elementN: valueN
    - filtertype: *second*
      filter_element1: value1
      ...
      filter_elementN: valueN
  2:
    action: ACTION2
    description: OPTIONAL DESCRIPTION
    options:
      option1: value1
      ...
      optionN: valueN
      continue_if_exception: False
      disable_action: True
    filters:
    - filtertype: *first*
      filter_element1: value1
      ...
      filter_elementN: valueN
    - filtertype: *second*
      filter_element1: value1
      ...
      filter_elementN: valueN
  3:
    action: ACTION3
    ...
  4:
    action: ACTION4
    ...
```

It is a YAML configuration file.  The root key must be `actions`, after which there can be any number of actions, nested underneath numbers.  Actions will be taken in the order they are completed.

The high-level elements of each numbered action are:

* [action](/reference/actions.md)
* [description](#description)
* [options](/reference/options.md)
* [filters](/reference/filters.md)

In the case of the [alias action](/reference/alias.md), there are two additional high-level elements: `add` and `remove`, which are described in the [alias action](/reference/alias.md) documentation.

## description [description]

This is an optional description which can help describe what the action and its filters are supposed to do.

```yaml
description: >- I can make the description span multiple
    lines by putting ">-" at the beginning of the line,
    as seen above.  Subsequent lines must also be indented.
options:
  option1: ...
```


