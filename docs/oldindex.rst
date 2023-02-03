Elasticsearch Curator
=====================

The Elasticsearch Curator helps you manage your indices and
snapshots.

.. note::

   This documentation is for Elasticsearch Curator.  Documentation specifically for use of the
   command-line interface -- which uses this API and is installed as an ``entry_point`` as part of
   the package -- is available in the `Elastic guide`_.

.. _Elastic guide: http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

Compatibility
*************

Elasticsearch Curator version 8 is compatible with Elasticsearch version 8.x, and supports Python
versions 3.8, 3.9, 3.10, and 3.11 officially.

Installation
************

Install the ``elasticsearch-curator`` package with `pip
<https://pypi.python.org/pypi/elasticsearch-curator>`_::

    pip install elasticsearch-curator

Example Usage
*************

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

Features
********

The API methods fall into the following categories:

* :doc:`Actions </actions>` act on object classes.
* :doc:`IndexList </indexlist>` build and filter a list of indices
* :doc:`SnapshotList </snapshotlist>` build and filter a list of snapshots
* :doc:`Utilities </utilities>` are helper methods.
* :doc:`Validators </validators>` validate configuration schemas.
* :doc:`Defaults </defaults>` constants or functions that return default values

Logging
*******

Elasticsearch Curator uses the standard `logging library`_ from Python.
It inherits two loggers from ``elasticsearch-py``: ``elasticsearch`` and
``elasticsearch.trace``. Clients use the ``elasticsearch`` logger to log
standard activity, depending on the log level. The ``elasticsearch.trace``
logger logs requests to the server in JSON format as pretty-printed ``curl``
commands that you can execute from the command line. The ``elasticsearch.trace``
logger is not inherited from the base logger and must be activated separately.

.. _logging library: http://docs.python.org/3.11/library/logging.html

Contents
********

.. toctree::
   :maxdepth: 1

   actions
   indexlist
   snapshotlist
   filters
   utilities
   validators
   defaults
   examples
   Changelog

License
*******

Copyright (c) 2011–2023 Elasticsearch <http://www.elastic.co>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Indices and Tables
******************

* :ref:`genindex`
* :ref:`search`
