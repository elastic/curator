.. _readme:


Curator
=======

Have indices in Elasticsearch? This is the tool for you!

Like a museum curator manages the exhibits and collections on display,
Elasticsearch Curator helps you curate, or manage your indices.


`Curator API Documentation`_
----------------------------

Curator ships with both an API and a wrapper script (which is actually defined
as an entry point).  The API allows you to write your own scripts to accomplish
similar goals, or even new and different things with the `Curator API`_, and
the `Elasticsearch Python API`_.

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

.. _Getting Started: https://www.elastic.co/guide/en/elasticsearch/client/curator/current/about.html

See the `Installation guide <https://www.elastic.co/guide/en/elasticsearch/client/curator/current/installation.html>`_
and the `command-line usage guide <https://www.elastic.co/guide/en/elasticsearch/client/curator/current/command-line.html>`_

Running ``curator --help`` will also show usage information.

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


Origins
-------

Curator was first called ``clearESindices.py`` [1]_ and was almost immediately
renamed to ``logstash_index_cleaner.py`` [1]_.  After a time it was migrated
under the `logstash <https://github.com/elastic/logstash>`_ repository as
``expire_logs``.  Soon thereafter, Jordan Sissel was hired by Elasticsearch, as
was the original author of this tool.  It became Elasticsearch Curator after
that and is now hosted at `elastic/curator <https://github.com/elastic/curator>`_.

.. [1] `LOGSTASH-211 <https://logstash.jira.com/browse/LOGSTASH-211>`_.
