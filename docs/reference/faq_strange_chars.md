---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/faq_strange_chars.html
---

# Q: Can Curator handle index names with strange characters? [faq_strange_chars]

## A: Yes! [_a_yes]

This problem can be resolved by using the [pattern filtertype](/reference/filtertype_pattern.md) with [kind](/reference/fe_kind.md) set to `regex`, and [value](/reference/fe_value.md) set to the needed regular expression.


#### The Problem: [_the_problem]

Illegal characters make it hard to delete indices.

```
% curl logs.example.com:9200/_cat/indices
red    }?ebc-2015.04.08.03
                          sip-request{ 5 1         0  0     632b     316b
red    }?ebc-2015.04.08.03
                          sip-response 5 1         0  0     474b     237b
red    ?ebc-2015.04.08.02
                         sip-request{ 5 1         0  0     474b     316b
red
eb                               5 1         0  0     632b     316b
red    ?e                                5 1         0  0     632b     316b
```


You can see it looks like there are some tab characters and maybe newline characters. This makes it hard to use the HTTP API to delete the indices.

Dumping all the index settings out:

```sh
curl -XGET localhost:9200/*/_settings?pretty
```


…​reveals the index names as the first key in the resulting JSON.  In this case, the names were very atypical:

```
}\b?\u0011ebc-2015.04.08.02\u000Bsip-request{
}\u0006?\u0011ebc-2015.04.08.03\u000Bsip-request{
}\u0003?\u0011ebc-2015.04.08.03\fsip-response
...
```


Curator lets you use regular expressions to select indices to perform actions on.

::::{warning}
Before attempting an action, see what will be affected by using the `--dry-run` flag first.
::::


To delete the first three from the above example, use `'.*sip.*'` as your regular expression.

::::{note}
In an [actionfile](/reference/actionfile.md), regular expressions and strftime date strings *must* be encapsulated in single-quotes.
::::


The next one is trickier. The real name of the index was `\n\u0011eb`. The regular expression `.*b$` did not work, but `'\n.*'` did.

The last index can be deleted with a regular expression of `'.*e$'`.

The resulting [actionfile](/reference/actionfile.md) might look like this:

```yaml
actions:
  1:
    description: Delete indices with strange characters that match regex '.*sip.*'
    action: delete_indices
    options:
      continue_if_exception: False
      disable_action: False
    filters:
    - filtertype: pattern
      kind: regex
      value: '.*sip.*'
  2:
    description: Delete indices with strange characters that match regex '\n.*'
    action: delete_indices
    options:
      continue_if_exception: False
      disable_action: False
    filters:
    - filtertype: pattern
      kind: regex
      value: '\n.*'
  3:
    description: Delete indices with strange characters that match regex '.*e$'
    action: delete_indices
    options:
      continue_if_exception: False
      disable_action: False
    filters:
    - filtertype: pattern
      kind: regex
      value: '.*e$'
```


