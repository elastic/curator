.. _examples:

Examples
========

Each of these examples presupposes that the requisite modules have been imported
and an instance of the Elasticsearch client object has been created:

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

Filter indices by prefix
++++++++++++++++++++++++

::

    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='prefix', value='logstash-')

The contents of `ilo.indices` would then only be indices matching the `prefix`.


Filter indices by suffix
++++++++++++++++++++++++

::

    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='suffix', value='-prod')

The contents of `ilo.indices` would then only be indices matching the `suffix`.


Filter indices by age (name)
++++++++++++++++++++++++++++

This example will match indices with the following criteria:

* Have a date string of ``%Y.%m.%d``
* Use `days` as the unit of time measurement
* Filter indices `older` than 5 `days`

::

    ilo = curator.IndexList(client)
    ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d',
        unit='days', unit_count=5
    )

The contents of `ilo.indices` would then only be indices matching these
criteria.


Filter indices by age (creation_date)
+++++++++++++++++++++++++++++++++++++

This example will match indices with the following criteria:

* Use `months` as the unit of time measurement
* Filter indices where the index creation date is `older` than 2 `months` from
  this moment.

::

    ilo = curator.IndexList(client)
    ilo.filter_by_age(source='creation_date', direction='older',
        unit='months', unit_count=2
    )

The contents of `ilo.indices` would then only be indices matching these
criteria.

Filter indices by age (field_stats)
+++++++++++++++++++++++++++++++++++

This example will match indices with the following criteria:

* Use `days` as the unit of time measurement
* Filter indices where the `timestamp` field's `min_value` is a date `older`
  than 3 `weeks` from this moment.


::

    ilo = curator.IndexList(client)
    ilo.filter_by_age(source='field_stats', direction='older',
        unit='weeks', unit_count=3, field='timestamp', stats_result='min_value'
    )

The contents of `ilo.indices` would then only be indices matching these
criteria.
