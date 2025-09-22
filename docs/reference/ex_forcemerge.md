---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_forcemerge.html
navigation_title: forcemerge
---

# Forcemerge examples in Elastic Curator [ex_forcemerge]

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
    action: forcemerge
    description: >-
      forceMerge logstash- prefixed indices older than 2 days (based on index
      creation_date) to 2 segments per shard.  Delay 120 seconds between each
      forceMerge operation to allow the cluster to quiesce. Skip indices that
      have already been forcemerged to the minimum number of segments to avoid
      reprocessing.
    options:
      max_num_segments: 2
      delay: 120
      timeout_override:
      continue_if_exception: False
      disable_action: True
    filters:
    - filtertype: pattern
      kind: prefix
      value: logstash-
      exclude:
    - filtertype: age
      source: creation_date
      direction: older
      unit: days
      unit_count: 2
      exclude:
    - filtertype: forcemerged
      max_num_segments: 2
      exclude:
```

