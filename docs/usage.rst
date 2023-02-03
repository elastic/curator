.. _usage:

Using Curator
#############

Compatibility
=============

Elasticsearch Curator version 8 is compatible with Elasticsearch version 8.x, and supports Python
versions 3.8, 3.9, 3.10, and 3.11 officially.

Installation
============

Install the ``elasticsearch-curator`` package with `pip
<https://pypi.python.org/pypi/elasticsearch-curator>`_::

    pip install elasticsearch-curator

Example Usage
=============

::

    import elasticsearch8
    import curator

    client = elasticsearch8.Elasticsearch()

    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='prefix', value='logstash-')
    ilo.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=30)
    delete_indices = curator.DeleteIndices(ilo)
    delete_indices.do_action()

.. TIP::
    See more examples in the :doc:`Examples </examples>` page.

Logging
=======

Elasticsearch Curator uses the standard `logging library`_ from Python.
It inherits two loggers from ``elasticsearch-py``: ``elasticsearch`` and
``elasticsearch.trace``. Clients use the ``elasticsearch`` logger to log
standard activity, depending on the log level. The ``elasticsearch.trace``
logger logs requests to the server in JSON format as pretty-printed ``curl``
commands that you can execute from the command line. The ``elasticsearch.trace``
logger is not inherited from the base logger and must be activated separately.

.. _logging library: http://docs.python.org/3.11/library/logging.html