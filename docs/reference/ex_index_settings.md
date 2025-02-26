---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_index_settings.html
---

# index_settings [ex_index_settings]

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
    action: index_settings
    description: >-
      Set Logstash indices older than 10 days to be read only (block writes)
    options:
      disable_action: True
      index_settings:
        index:
          blocks:
            write: True
      ignore_unavailable: False
      preserve_existing: False
    filters:
    - filtertype: pattern
      kind: prefix
      value: logstash-
      exclude:
    - filtertype: age
      source: name
      direction: older
      timestring: '%Y.%m.%d'
      unit: days
      unit_count: 10
```

