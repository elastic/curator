---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_delete_snapshots.html
---

# delete_snapshots [ex_delete_snapshots]

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
    action: delete_snapshots
    description: >-
      Delete snapshots from the selected repository older than 45 days
      (based on creation_date), for 'curator-' prefixed snapshots.
    options:
      repository:
      disable_action: True
    filters:
    - filtertype: pattern
      kind: prefix
      value: curator-
      exclude:
    - filtertype: age
      source: creation_date
      direction: older
      unit: days
      unit_count: 45
```

