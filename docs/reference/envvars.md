---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/envvars.html
---

# Environment Variables [envvars]

::::{warning}
This functionality is experimental and may be changed or removed<br> completely in a future release.
::::


You can use environment variable references in both the [configuration file](/reference/configfile.md) and the [action file](/reference/actionfile.md) to set values that need to be configurable at runtime. To do this, use:

```sh
${VAR}
```

Where `VAR` is the name of the environment variable.

Each variable reference is replaced at startup by the value of the environment variable. The replacement is case-sensitive and occurs while the YAML file is parsed, but before configuration schema validation. References to undefined variables are replaced by `None` unless you specify a default value. To specify a default value, use:

```sh
${VAR:default_value}
```

Where `default_value` is the value to use if the environment variable is undefined.

::::{admonition} Unsupported use cases
:class: important

When using environment variables, the value must *only* be the environment variable.

Using extra text, such as:

```sh
logfile: ${LOGPATH}/extra/path/information/file.log
```

is not supported at this time.

::::


## Examples [_examples]

Here are some examples of configurations that use environment variables and what each configuration looks like after replacement:

| Config source | Environment setting | Config after replacement |
| --- | --- | --- |
| `unit: ${UNIT}` | `export UNIT=days` | `unit: days` |
| `unit: ${UNIT}` | no setting | `unit:` |
| `unit: ${UNIT:days}` | no setting | `unit: days` |
| `unit: ${UNIT:days}` | `export UNIT=hours` | `unit: hours` |


