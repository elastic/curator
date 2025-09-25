---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_create_index.html
---

# create_index [ex_create_index]

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
    action: create_index
    description: Create the index as named, with the specified extra settings.
    options:
      name: myindex
      extra_settings:
        settings:
          number_of_shards: 2
          number_of_replicas: 1
      disable_action: True
```

