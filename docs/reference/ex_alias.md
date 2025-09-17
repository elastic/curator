---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_alias.html
---

# Alias action example in Elasticsearch Curator [ex_alias]

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
    action: alias
    description: >-
      Alias indices from last week, with a prefix of logstash- to 'last_week',
      remove indices from the previous week.
    options:
      name: last_week
      warn_if_no_indices: False
      disable_action: True
    add:
      filters:
      - filtertype: pattern
        kind: prefix
        value: logstash-
        exclude:
      - filtertype: period
        period_type: relative
        source: name
        range_from: -1
        range_to: -1
        timestring: '%Y.%m.%d'
        unit: weeks
        week_starts_on: sunday
    remove:
      filters:
      - filtertype: pattern
        kind: prefix
        value: logstash-
      - filtertype: period
        period_type: relative
        source: name
        range_from: -2
        range_to: -2
        timestring: '%Y.%m.%d'
        unit: weeks
        week_starts_on: sunday
```

