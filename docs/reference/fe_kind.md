---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_kind.html
---

# kind [fe_kind]

::::{note}
This setting is only used with the [pattern](/reference/filtertype_pattern.md)<br> filtertype and is a required setting.
::::


This setting tells the [pattern](/reference/filtertype_pattern.md) what pattern type to match. Acceptable values for this setting are `prefix`, `suffix`, `timestring`, and `regex`.

::::{admonition} Filter chaining
:class: note

It is important to note that while filters can be chained, each is linked by an implied logical **AND** operation.  If you want to match from one of several different patterns, as with a logical **OR** operation, you can do so with the [pattern](/reference/filtertype_pattern.md) filtertype using *regex* as the `kind`.

This example shows how to select multiple indices based on them beginning with either `alpha-`, `bravo-`, or `charlie-`:

```yaml
  filters:
  - filtertype: pattern
    kind: regex
    value: '^(alpha-|bravo-|charlie-).*$'
```

Explaining all of the different ways in which regular expressions can be used is outside the scope of this document, but hopefully this gives you some idea of how a regular expression pattern can be used when a logical **OR** is desired.

::::


There is no default value. This setting must be set by the user or an exception will be raised, and execution will halt.

The different `kinds` are described as follows:

## prefix [_prefix_2]

To match all indices starting with `logstash-`:

```yaml
- filtertype: pattern
 kind: prefix
 value: logstash-
```

To match all indices *except* those starting with `logstash-`:

```yaml
- filtertype: pattern
 kind: prefix
 value: logstash-
 exclude: True
```

::::{note}
Internally, the `prefix` value is used to create a *regex* pattern: `^{{0}}.*$`. Any special characters should be escaped with a backslash to match literally.
::::



## suffix [_suffix_2]

To match all indices ending with `-prod`:

```yaml
- filtertype: pattern
 kind: suffix
 value: -prod
```

To match all indices *except* those ending with `-prod`:

```yaml
- filtertype: pattern
 kind: suffix
 value: -prod
 exclude: True
```

::::{note}
Internally, the `suffix` value is used to create a *regex* pattern: `^.*{{0}}$`. Any special characters should be escaped with a backslash to match literally.
::::



## timestring [_timestring_2]

::::{important}
No age calculation takes place here. It is strictly a pattern match.
::::


To match all indices with a Year.month.day pattern, like `index-2017.04.01`:

```yaml
- filtertype: pattern
 kind: timestring
 value: '%Y.%m.%d'
```

To match all indices *except* those with a Year.month.day pattern, like `index-2017.04.01`:

```yaml
- filtertype: pattern
 kind: timestring
 value: '%Y.%m.%d'
 exclude: True
```

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



## regex [_regex_2]

This `kind` allows you to design a regular-expression to match indices or snapshots:

To match all indices starting with `a-`, `b-`, or `c-`:

```yaml
- filtertype: pattern
 kind: regex
 value: '^a-|^b-|^c-'
```

To match all indices *except* those starting with `a-`, `b-`, or `c-`:

```yaml
- filtertype: pattern
 kind: regex
 value: '^a-|^b-|^c-'
 exclude: True
```


