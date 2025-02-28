---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_aliases.html
---

# aliases [fe_aliases]

::::{admonition} Matching Indices and Aliases
:class: important

[Indices must be in all aliases to match](https://www.elastic.co/guide/en/elasticsearch/reference/5.5/breaking-changes-5.5.html#breaking_55_rest_changes).

If a list of `aliases` is provided (instead of only one), indices must appear in *all* listed `aliases` or a 404 error will result, leading to no indices being matched. In older versions, if the index was associated with even one of the aliases in `aliases`, it would result in a match.

::::


::::{note}
This setting is used only when using the [alias](/reference/filtertype_alias.md) filter.
::::


The value of this setting must be a single alias name, or a list of alias names. This can be done in any of the ways YAML allows for lists or arrays.  Here are a few examples.

**Single**

```txt
filters:
- filtertype: alias
  aliases: my_alias
  exclude: False
```

**List**

* Flow style:

    ```txt
    filters:
    - filtertype: alias
      aliases: [ my_alias, another_alias ]
      exclude: False
    ```

* Block style:

    ```txt
    filters:
    - filtertype: alias
      aliases:
        - my_alias
        - another_alias
      exclude: False
    ```


There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.
