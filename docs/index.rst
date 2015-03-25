Elasticsearch Curator Python API
================================

The Elasticsearch Curator Python API helps you manage your indices and
snapshots.

.. note::

   This documentation is for the Elasticsearch Curator Python API.  Documentation
   for the Elasticsearch Curator *CLI* -- which uses this API and is installed
   as an entry_point as part of the package -- is available in the
   `Elastic guide`_.

.. _Elastic guide: http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

Compatibility
-------------

The Elasticsearch Curator Python API is compatible with Elasticsearch versions 1.x
through 2.0, and supports Python versions 2.6 and later.

Example Usage
-------------

::

    import elasticsearch
    import curator

    client = elasticsearch.Elasticsearch()

    curator.close_indices(client, ['logstash-2014.08.16','logstash-2014.08.17'])
    curator.disable_bloom_filter(client, 'logstash-2014.08.31')
    curator.optimize_index(client, 'logstash-2014.08.31')
    curator.delete(client, ['logstash-2014.07.16', 'logstash-2014.07.17'])

.. TIP::
    See more examples in the :doc:`Examples </examples>` page.

Features
--------

The API methods fall into the following categories:

* :doc:`Commands </commands>` take a single index, or in some cases a list of indices and perform an action on them.
* :doc:`Filters </filters>` are there to filter indices or snapshots based on provided criteria.
* :doc:`Utilities </utilities>` are helper methods for commands and filters.

Filtering indices is now handled by the :py:func:`curator.api.build_filter` method.

Logging
~~~~~~~

The Elasticsearch Curator Python API uses the standard `logging library`_ from Python.
It inherits two loggers from ``elasticsearch-py``: ``elasticsearch`` and
``elasticsearch.trace``. Clients use the ``elasticsearch`` logger to log
standard activity, depending on the log level. The ``elasticsearch.trace``
logger logs requests to the server in JSON format as pretty-printed ``curl``
commands that you can execute from the command line. The ``elasticsearch.trace``
logger is not inherited from the base logger and must be activated separately.

.. _logging library: http://docs.python.org/3.3/library/logging.html

Contents
--------

.. toctree::
   :maxdepth: 2

   commands
   filters
   utilities
   examples
   Changelog

License
-------

Copyright 2013–2015 Elastic <http://elastic.co> and contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`
