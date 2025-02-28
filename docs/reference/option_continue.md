---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_continue.html
---

# continue_if_exception [option_continue]

::::{admonition} Using `ignore_empty_list` rather than `continue_if_exception`
:class: important

Curator has two general classifications of exceptions: Empty list exceptions, and everything else. The empty list conditions are `curator.exception.NoIndices` and `curator.exception.NoSnapshots`.  The `continue_if_exception` option *only* catches conditions *other* than empty list conditions. In most cases, you will want to use `ignore_empty_list` instead of `continue_if_exception`.

So why are there two kinds of exceptions? When Curator 4 was released, the ability to continue in the event of any exception was covered by the `continue_if_exception` option.  However, an empty list is a *benign* condition. In fact, it’s expected with brand new clusters, or when new index patterns are added. The decision was made to split the exceptions, and have a new option catch the empty lists.

See [`ignore_empty_list`](/reference/option_ignore_empty.md) for more information.

::::


::::{note}
This setting is available in all actions.
::::


```yaml
action: delete_indices
description: "Delete selected indices"
options:
  continue_if_exception: False
filters:
- filtertype: ...
```

If `continue_if_exception` is set to `True`, Curator will attempt to continue on to the next action, if any, even if an exception is encountered. Curator will log but ignore the exception that was raised.

The default value for this setting is `False`

