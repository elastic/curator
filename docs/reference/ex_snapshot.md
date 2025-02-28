---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_snapshot.html
---

# snapshot [ex_snapshot]

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
    action: snapshot
    description: >-
      Snapshot logstash- prefixed indices older than 1 day (based on index
      creation_date) with the default snapshot name pattern of
      'curator-%Y%m%d%H%M%S'.  Wait for the snapshot to complete.  Do not skip
      the repository filesystem access check.  Use the other options to create
      the snapshot.
    options:
      repository:
      # Leaving name blank will result in the default 'curator-%Y%m%d%H%M%S'
      name:
      ignore_unavailable: False
      include_global_state: True
      partial: False
      wait_for_completion: True
      skip_repo_fs_check: False
      disable_action: True
    filters:
    - filtertype: pattern
      kind: prefix
      value: logstash-
    - filtertype: age
      source: creation_date
      direction: older
      unit: days
      unit_count: 1
```

