.. _readme:


Curator
=======

Have indices in Elasticsearch? This is the tool for you!

Like a museum curator manages the exhibits and collections on display,
Elasticsearch Curator helps you curate, or manage your indices.

Compatibility Matrix
=======

+--------+----------+----------+----------+
|Version | ES 1.x   | ES 2.x   | ES 5.x   |
+========+==========+==========+==========+
|    3   |    yes   |     yes  |     no   |
+--------+----------+----------+----------+
|    4   |    no    |     yes  |     yes  |
+--------+----------+----------+----------+

It is important to note that Curator 4 will not work with indices created in
versions of Elasticsearch older than 1.4 (if they have been subsequently
re-indexed, they will work).  This is because those older indices lack index
metadata that Curator 4 requires.  Curator 4 will simply exclude any such
indices from being acted on, and you will get a warning message like the
following:

::

    2016-07-31 10:36:17,423 WARNING Index: YOUR_INDEX_NAME has no
    "creation_date"! This implies that the index predates Elasticsearch v1.4.
    For safety, this index will be removed from the actionable list.



Build Status
------------

+--------+----------+
| Branch | Status   |
+========+==========+
| Master | |master| |
+--------+----------+
| 4.x    | |4_x|    |
+--------+----------+
| 4.0    | |4_0|    |
+--------+----------+

PyPI: |pypi_pkg|

.. |master| image:: https://travis-ci.org/elastic/curator.svg?branch=master
    :target: https://travis-ci.org/elastic/curator
.. |4_x| image:: https://travis-ci.org/elastic/curator.svg?branch=4.x
    :target: https://travis-ci.org/elastic/curator
.. |4_0| image:: https://travis-ci.org/elastic/curator.svg?branch=4.0
    :target: https://travis-ci.org/elastic/curator
.. |pypi_pkg| image:: https://badge.fury.io/py/elasticsearch-curator.svg
    :target: https://badge.fury.io/py/elasticsearch-curator

`Curator API Documentation`_
----------------------------

Version 4.0 of Curator ships with both an API and a wrapper script (which is
actually defined as an entry point).  The API allows you to write your own
scripts to accomplish similar goals, or even new and different things with the
`Curator API`_, and the `Elasticsearch Python API`_.

.. _Curator API: http://curator.readthedocs.io/

.. _Curator API Documentation: `Curator API`_

.. _Elasticsearch Python API: http://elasticsearch-py.readthedocs.io/

`Curator CLI Documentation`_
----------------------------

The `Curator CLI Documentation`_ is now a part of the document repository at
http://elastic.co/guide at http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

.. _Curator CLI Documentation: http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

`Getting Started`_
------------------

.. _Getting Started: https://www.elastic.co/guide/en/elasticsearch/client/curator/current/getting-started.html

See the `Installation guide <https://www.elastic.co/guide/en/elasticsearch/client/curator/current/installation.html>`_
and the `command-line usage guide <https://www.elastic.co/guide/en/elasticsearch/client/curator/current/command-line.html>`_

Running ``curator --help`` will also show usage information.

`Frequently Asked Questions`_
-----------------------------

.. _Frequently Asked Questions: http://www.elastic.co/guide/en/elasticsearch/client/curator/current/faq.html

Encountering issues like ``DistributionNotFound``? See the FAQ_ for that issue, and more.

.. _FAQ: http://www.elastic.co/guide/en/elasticsearch/client/curator/current/entrypoint-fix.html

`Documentation & Examples`_
---------------------------

.. _Documentation & Examples: http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

The documentation for the CLI is now part of the document repository at http://elastic.co/guide
at http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

The `Curator Wiki <http://github.com/elastic/curator/wiki>`_ on Github is now a
place to add your own examples and ideas.

Contributing
------------

* fork the repo
* make changes in your fork
* add tests to cover your changes (if necessary)
* run tests
* sign the `CLA <http://elastic.co/contributor-agreement/>`_
* send a pull request!

To run from source, use the ``run_curator.py`` script in the root directory of
the project.

Running Tests
-------------

To run the test suite just run ``python setup.py test``

When changing code, contributing new code or fixing a bug please make sure you
include tests in your PR (or mark it as without tests so that someone else can
pick it up to add the tests). When fixing a bug please make sure the test
actually tests the bug - it should fail without the code changes and pass after
they're applied (it can still be one commit of course).

The tests will try to connect to your local elasticsearch instance and run
integration tests against it. This will delete all the data stored there! You
can use the env variable ``TEST_ES_SERVER`` to point to a different instance
(for example, 'otherhost:9203').

Versioning
----------

Version 4.0 of Curator is the current ``master`` branch.  It supports
Elasticsearch versions 2.0 through 5.0.  This is the first release of Curator
that is not fully reverse compatible.

The ``3.x`` branch will continue to be available to support earlier versions of
Elasticsearch. No new development is being done with the ``3.x`` branch, but bug
fixes may be merged as necessary.

Origins
-------

Curator was first called ``clearESindices.py`` [1] and was almost immediately
renamed to ``logstash_index_cleaner.py`` [1].  After a time it was migrated under
the [logstash](https://github.com/elastic/logstash) repository as
``expire_logs``.  Soon thereafter, Jordan Sissel was hired by Elasticsearch, as
was the original author of this tool.  It became Elasticsearch Curator after
that and is now hosted at <https://github.com/elastic/curator>

[1] <https://logstash.jira.com/browse/LOGSTASH-211>
