.. _examples:

Examples
========

`build_filter` Examples
---------------------

Filter indices by prefix
++++++++++++++++++++++++

This example will generate a list of indices matching the prefix, 'logstash'.
The effective regular expression would be: `^logstash.*$`
`_filter` is a dictionary object.  We send it as key word arguments (kwargs) to
`apply_filter`.  `_filter`'s contents: ``{'pattern': '^logstash.*$'}``

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    _filter = curator.build_filter(kindOf='prefix', value='logstash')
    working_list = curator.apply_filter(indices, **_filter)

The contents of `working_list` would then only be indices matching the `prefix`.


Filter indices by suffix
++++++++++++++++++++++++

This example will generate a list of indices matching the suffix, '-prod'.
The effective regular expression would be: `^.*-prod$`
`_filter` is a dictionary object.  We send it as key word arguments (kwargs) to
`apply_filter`.  `_filter`'s contents: ``{'pattern': '^.*-prod$'}``

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    _filter = curator.build_filter(kindOf='suffix', value='-prod')
    working_list = curator.apply_filter(indices, **_filter)

The contents of `working_list` would then only be indices matching the `suffix`.


Filter indices by time (older_than)
+++++++++++++++++++++++++++++++++++

This example will generate a list of indices matching the following criteria:

* Have a date string of ``%Y.%m.%d``
* Use `days` as the unit of time measurement
* Filter indices `older_than` 5 `days`

`_filter` is a dictionary object.  We send it as key word arguments (kwargs) to
`apply_filter`.

The resulting `_filter` dictionary will be:

::

    {
          'pattern': '(?P<date>\\d{4}\\.\\d{2}\\.\\d{2})', 'value': 5,
          'groupname': 'date', 'time_unit': 'days',
          'timestring': '%Y.%d.%m', 'method': 'older_than'
    }

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    _filter = curator.build_filter(kindOf='suffix', value='-prod')
    working_list = curator.apply_filter(indices, **_filter)

The contents of `working_list` would then only be indices matching these
criteria.


Filter indices by time (newer_than)
+++++++++++++++++++++++++++++++++++

This example will generate a list of indices matching the following criteria:

* Have a date string of ``%Y.%m.%d``
* Use `days` as the unit of time measurement
* Filter indices `newer_than` 5 `days`

`_filter` is a dictionary object.  We send it as key word arguments (kwargs) to
`apply_filter`.

The resulting `_filter` dictionary will be:

::

    {
          'pattern': '(?P<date>\\d{4}\\.\\d{2}\\.\\d{2})', 'value': 5,
          'groupname': 'date', 'time_unit': 'days',
          'timestring': '%Y.%d.%m', 'method': 'newer_than'
    }

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    _filter = curator.build_filter(
                kindOf='newer_than', value=5, time_unit='days',
                timestring='%Y.%d.%m'
             )
    working_list = curator.apply_filter(indices, **_filter)

The contents of `working_list` would then only be indices matching these
criteria.


Filter indices by custom regular expression
+++++++++++++++++++++++++++++++++++++++++++

This example will generate a list of indices matching a custom regular
expression ``(your expression)``.

``(your expression)`` needs to be a valid regular expression.

`_filter` is a dictionary object.  We send it as key word arguments (kwargs) to
`apply_filter`.  `_filter`'s contents: ``{'pattern': (your expression)}``

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    _filter = curator.build_filter(kindOf='regex', value=(your expression))
    working_list = curator.apply_filter(indices, **_filter)

The contents of `working_list` would then only be indices matching
(your expression)


Filter indices by excluding matches
+++++++++++++++++++++++++++++++++++

This example will generate a list of all indices `not` matching the pattern,
'dev-'.

The effective regular expression would be: `^dev-.*$`

`_filter` is a dictionary object.  We send it as key word arguments (kwargs) to
`apply_filter`.  `_filter`'s contents: ``{'pattern': 'dev-', 'exclude': True}``

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    _filter = curator.build_filter(kindOf='exclude', value='dev-')
    working_list = curator.apply_filter(indices, **_filter)

The contents of `working_list` would then be all indices not matching the
pattern, 'dev-'.

.. note::

    Any filter can become an `exclude` by adding ``'exclude':True`` to the
    `_filter` dictionary.

Filter indices by time string as a pattern
++++++++++++++++++++++++++++++++++++++++++

This example will generate a list of indices having a matching time string,
where `value` must be a valid python strftime string.

`_filter` is a dictionary object.  We send it as key word arguments (kwargs) to
`apply_filter`.  `_filter`'s contents:
``{'pattern': '(?P<date>\\d{4}\\.\\d{2}\\.\\d{2})'}``

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    _filter = curator.build_filter(kindOf='timestring', value='%Y.%m.%d')
    working_list = curator.apply_filter(indices, **_filter)

The contents of `working_list` would then only be indices having a matching
time string.


More complex example mimicking the CLI
++++++++++++++++++++++++++++++++++++++

This example will show time-series indices matching `prefix`, `older_than` 30
`days` (the `time_unit`), and `newer_than` 60 `days`.

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    indices = curator.get_indices(client)
    filter_list = []
    filter_list.append(curator.build_filter(kindOf='prefix', value='logstash'))
    filter_list.append(
        curator.build_filter(
            kindOf='older_than', value=30, time_unit='days',
            timestring='%Y.%d.%m'
        )
    )
    filter_list.append(
        curator.build_filter(
            kindOf='newer_than', value=60, time_unit='days',
            timestring='%Y.%d.%m'
        )
    )
    working_list = indices
    for filter in filter_list:
        working_list = curator.apply_filter(working_list, **filter)
    curator.show(working_list)
