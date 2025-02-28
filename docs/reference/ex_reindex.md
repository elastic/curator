---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ex_reindex.html
---

# reindex [ex_reindex]

## Manually selected reindex of a single index [_manually_selected_reindex_of_a_single_index]

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
    description: "Reindex index1 into index2"
    action: reindex
    options:
      disable_action: True
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```


## Manually selected reindex of a multiple indices [_manually_selected_reindex_of_a_multiple_indices]

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
    description: "Reindex index1,index2,index3 into new_index"
    action: reindex
    options:
      disable_action: True
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: ['index1', 'index2', 'index3']
        dest:
          index: new_index
    filters:
    - filtertype: none
```


## Filter-Selected Indices [_filter_selected_indices_2]

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
    description: >-
      'Reindex all daily logstash indices from March 2017 into logstash-2017.03'
    action: reindex
    options:
      disable_action: True
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: REINDEX_SELECTION
        dest:
          index: logstash-2017.03
    filters:
    - filtertype: pattern
      kind: prefix
      value: logstash-2017.03.
```


## Reindex From Remote [_reindex_from_remote_2]

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
    description: >-
      'Reindex all daily logstash indices from March 2017 into logstash-2017.03'
    action: reindex
    options:
      disable_action: True
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          remote:
            host: http://otherhost:9200
            username: myuser
            password: mypass
          index: index1
        dest:
          index: index1
    filters:
    - filtertype: none
```


## Reindex From Remote With Filter-Selected Indices [_reindex_from_remote_with_filter_selected_indices_2]

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
    description: >-
      Reindex all remote daily logstash indices from March 2017 into local index
      logstash-2017.03
    action: reindex
    options:
      disable_action: True
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          remote:
            host: http://otherhost:9200
            username: myuser
            password: mypass
          index: REINDEX_SELECTION
        dest:
          index: logstash-2017.03
      remote_filters:
      - filtertype: pattern
      kind: prefix
      value: logstash-2017.03.
    filters:
    - filtertype: none
```


## Manually selected reindex of a single index with query [_manually_selected_reindex_of_a_single_index_with_query]

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
    description: "Reindex index1 into index2"
    action: reindex
    options:
      disable_action: True
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          query:
            range:
              timestamp:
                gte: "now-1h"
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```


