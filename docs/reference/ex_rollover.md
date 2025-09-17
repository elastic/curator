---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_rollover.html
navigation_title: rollover
---

# rollover action examples [ex_rollover]

```yaml
---
# Remember, leave a key empty if there is no value.  None will be a string,
# not a Python "NoneType"
#
# Also remember that all examples have 'disable_action' set to True.  If you
# want to use this action as a template, be sure to set this to False after
# copying it.
actions:
  1:
    action: rollover
    description: >-
      Rollover the index associated with alias 'aliasname', which should be in the
      format of prefix-000001 (or similar), or prefix-YYYY.MM.DD-1.
    options:
      disable_action: True
      name: aliasname
      conditions:
        max_age: 1d
        max_docs: 1000000
        max_size: 50g
      extra_settings:
        index.number_of_shards: 3
        index.number_of_replicas: 1
```

