---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/filtertype_pattern.html
---

# pattern [filtertype_pattern]

```yaml
- filtertype: pattern
 kind: ...
 value: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given [filtertype](/reference/filtertype.md), it may generate an error.
::::


This [filtertype](/reference/filtertype.md) will iterate over the actionable list and match indices matching a given pattern.  They will remain in, or be removed from the actionable list based on the value of [exclude](/reference/fe_exclude.md).

::::{admonition} Filter chaining
:class: note

It is important to note that while filters can be chained, each is linked by an implied logical **AND** operation.  If you want to match from one of several different patterns, as with a logical **OR** operation, you can do so with the pattern filtertype using *regex* as the [kind](/reference/fe_kind.md).

This example shows how to select multiple indices based on them beginning with either `alpha-`, `bravo-`, or `charlie-`:

```yaml
  filters:
  - filtertype: pattern
    kind: regex
    value: '^(alpha-|bravo-|charlie-).*$'
```

Explaining all of the different ways in which regular expressions can be used is outside the scope of this document, but hopefully this gives you some idea of how a regular expression pattern can be used when a logical **OR** is desired.

::::


The different [`kinds`](/reference/fe_kind.md) are described as follows:

## prefix [_prefix]

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



## suffix [_suffix]

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



## timestring [_timestring]

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



## regex [_regex]

This [`kind`](/reference/fe_kind.md) allows you to design a regular-expression to match indices or snapshots:

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


## Required settings [_required_settings_18]

* [kind](/reference/fe_kind.md)
* [value](/reference/fe_value.md)


## Optional settings [_optional_settings_27]

* [exclude](/reference/fe_exclude.md) (default is `False`)


