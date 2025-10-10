---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_timestring.html
---

# timestring [fe_timestring]

::::{note}
This setting is only used with the [age](/reference/filtertype_age.md) filtertype, or<br> with the [space](/reference/filtertype_space.md) filtertype if [use_age](/reference/fe_use_age.md) is set to `True`.
::::


## strftime [_strftime_2]

This setting must be a valid Python strftime string.  It is used to match and extract the timestamp in an index or snapshot name.

The identifiers that Curator currently recognizes include:

| Unit | Value | Note |
| --- | --- | --- |
| `%Y` | 4 digit year |  |
| `%G` | 4 digit year | use instead of `%Y` when doing ISO Week calculations |
| `%y` | 2 digit year |  |
| `%m` | 2 digit month |  |
| `%W` | 2 digit week of the year |  |
| `%V` | 2 digit week of the year | use instead of `%W` when doing ISO Week calculations |
| `%d` | 2 digit day of the month |  |
| `%H` | 2 digit hour | 24 hour notation |
| `%M` | 2 digit minute |  |
| `%S` | 2 digit second |  |
| `%j` | 3 digit day of the year |  |

These identifiers may be combined with each other, and/or separated from each other with hyphens `-`, periods `.`, underscores `_`, or other characters valid in an index name.

Each identifier must be preceded by a `%` character in the timestring.  For example, an index like `index-2016.04.01` would use a timestring of `'%Y.%m.%d'`.

When [source](/reference/fe_source.md) is `name`, this setting must be set by the user or an exception will be raised, and execution will halt. There is no default value.

::::{admonition} A word about regular expression matching with timestrings
:class: warning

Timestrings are parsed from strftime patterns, like `%Y.%m.%d`, into regular expressions.  For example, `%Y` is 4 digits, so the regular expression for that looks like `\d{{4}}`, and `%m` is 2 digits, so the regular expression is `\d{{2}}`.

What this means is that a simple timestring to match year and month, `%Y.%m` will result in a regular expression like this: `^.*\d{{4}}\.\d{{2}}.*$`.  This pattern will match any 4 digits, followed by a period `.`, followed by 2 digits, occurring anywhere in the index name.  This means it *will* match monthly indices, like `index-2016.12`, as well as daily indices, like `index-2017.04.01`, which may not be the intended behavior.

To compensate for this, when selecting indices matching a subset of another pattern, use a second filter with `exclude` set to `True`

```yaml
- filtertype: pattern
 kind: timestring
 value: '%Y.%m'
- filtertype: pattern
 kind: timestring
 value: '%Y.%m.%d'
 exclude: True
```

This will prevent the `%Y.%m` pattern from matching the `%Y.%m` part of the daily indices.

**This applies whether using `timestring` as a mere pattern match, or as part of date calculations.**

::::



