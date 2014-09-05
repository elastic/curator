Elasticsearch Curator API
=========================

The Elasticsearch Curator API helps you manage time-series indices and
snapshots.

.. note::

   This documentation is for the Elasticsearch Curator API.  Documentation for
   the Elasticsearch Curator *script*, which uses this API and is installed as
   part of the package is available on the `the wiki`_.

.. _the wiki: http://github.com/elasticsearch/curator/wiki

Compatibility
-------------

The Elasticsearch Curator API is compatible with Elasticsearch versions 1.x
through 2.0, and supports Python versions 2.6 and later.

Example Usage
-------------

::

    import elasticsearch
    import curator
    
    client = elasticsearch.Elasticsearch()
    
    curator.close_index(client, 'logstash-2014.08.16')
    curator.disable_bloom_filter(client, 'logstash-2014.08.31')
    curator.optimize_index(client, 'logstash-2014.08.31')
    curator.delete(client, older_than=30, time_unit='days', prefix='logstash-')

Features
--------

The API methods fall into the following categories:

* :doc:`Iterative methods </iterative>` run against all of the indices on your cluster matching given patterns. You can restrict these matches to indices before a specified time.
* :doc:`Non-iterative methods </non-iterative>` operate against a single index or snapshot at a time.
* :doc:`Helper methods </helpers>` provide information and values that the iterative and non-iterative methods need to complete.

Logging
~~~~~~~

The Elasticsearch Curator API uses the standard `logging library`_ from Python.
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

   iterative
   non-iterative
   helpers
   Changelog

License
-------

Copyright 2013-2014 Elasticsearch

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

