.. _readme:


Curator
=======

Have indices in Elasticsearch? This is the tool for you!

Like a museum curator manages the exhibits and collections on display,
Elasticsearch Curator helps you curate, or manage your indices.

Compatibility Matrix
====================

+--------+----------+------------+----------+------------+----------+------------+
|Version | ES 1.x   | AWS ES 1.x | ES 2.x   | AWS ES 2.x | ES 5.x   | AWS ES 5.x |
+========+==========+============+==========+============+==========+============+
|    3   |    yes   |     yes*   |   yes    |     yes*   |   no     |     no     |
+--------+----------+------------+----------+------------+----------+------------+
|    4   |    no    |     no     |   yes    |     no     |   yes    |     no     |
+--------+----------+------------+----------+------------+----------+------------+
|    5   |    no    |     no     |   no     |     no     |   yes    |     yes*   |
+--------+----------+------------+----------+------------+----------+------------+


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

It is also important to note that Curator 4 requires access to the
``/_cluster/state/metadata`` endpoint.  Forks of Elasticsearch which do not
support this endpoint (such as AWS ES, see #717) *will not* be able to use
Curator version 4.

\* It appears that AWS ES `does not allow access to the snapshot status endpoint`_ 
for the 1.x, 2.x, 5.1, and 5.3 versions.  This prevents Curator 3 from being 
used to make snapshots.

.. _does not allow access to the snapshot status endpoint: https://github.com/elastic/curator/issues/796

? Curator 4 and 5 should work with AWS ES 5.x, but the 
``/_cluster/state/metadata`` endpoint is still not fully supported (see #880).
If a future patch fixes this, then Curator 4 and 5 should work with AWS ES 5.x.

Build Status
------------

+--------+----------+
| Branch | Status   |
+========+==========+
| Master | |master| |
+--------+----------+
| 5.x    | |5_x|    |
+--------+----------+
| 5.1    | |5_1|    |
+--------+----------+
| 5.0    | |5_0|    |
+--------+----------+
| 4.x    | |4_x|    |
+--------+----------+
| 4.3    | |4_3|    |
+--------+----------+

PyPI: |pypi_pkg|

.. |master| image:: https://travis-ci.org/elastic/curator.svg?branch=master
    :target: https://travis-ci.org/elastic/curator
.. |5_x| image:: https://travis-ci.org/elastic/curator.svg?branch=5.x
    :target: https://travis-ci.org/elastic/curator
.. |5_1| image:: https://travis-ci.org/elastic/curator.svg?branch=5.1
    :target: https://travis-ci.org/elastic/curator
.. |5_0| image:: https://travis-ci.org/elastic/curator.svg?branch=5.0
    :target: https://travis-ci.org/elastic/curator
.. |4_x| image:: https://travis-ci.org/elastic/curator.svg?branch=4.x
    :target: https://travis-ci.org/elastic/curator
.. |4_3| image:: https://travis-ci.org/elastic/curator.svg?branch=4.3
    :target: https://travis-ci.org/elastic/curator
.. |pypi_pkg| image:: https://badge.fury.io/py/elasticsearch-curator.svg
    :target: https://badge.fury.io/py/elasticsearch-curator

`Curator API Documentation`_
----------------------------

Version 5 of Curator ships with both an API and a wrapper script (which is
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

.. _Getting Started: https://www.elastic.co/guide/en/elasticsearch/client/curator/current/about.html

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

Binary Executables
------------------

The combination of `setuptools <https://github.com/pypa/setuptools>`_ and
`cx_Freeze <http://cx-freeze.sourceforge.net>`_ allows for Curator to be
compiled into binary packages.  These consist of a binary file placed in a
directory which contains all the libraries required to run it.

In order to make a binary package you must manually install the ``cx_freeze``
python module.  You can do this via ``pip``, or ``python setup.py install``,
or by package, if such exists for your platform.  In order to make it compile on
recent Debian/Ubuntu platforms, a patch had to be applied to the ``setup.py``
file in the extracted folder.  This patch file is in the ``unix_packages``
directory in this repository.

With ``cx_freeze`` installed, building a binary package is as simple as running
``python setup.py build_exe``.  In Linux distributions, the results will be in
the ``build`` directory, in a subdirectory labelled
``exe.linux-x86_64-${PYVER}``, where `${PYVER}` is the current major/minor
version of Python, e.g. ``2.7``.  This directory can be renamed as desired.

Other entry-points that are defined in the ``setup.py`` file, such as
``es_repo_mgr``, will also appear in this directory.

The process is identical for building the binary package for Windows.  It must
be run from a Windows machine with all dependencies installed.  Executables in
Windows will have the ``.exe`` suffix attached.  The directory in ``build`` will
be named ``exe.win-amd64-${PYVER}``, where `${PYVER}` is the current major/minor
version of Python, e.g. ``2.7``.  This directory can be renamed as desired.

In Windows, cx_Freeze also allows for building rudimentary MSI installers.  This
can be done by invoking ``python setup.py bdist_msi``.  The MSI fill will be in
the ``dist`` directory, and will be named
``elasticsearch-curator-#.#.#-amd64.msi``, where the major, minor, and patch
version numbers are substituted accordingly.  One drawback to this rudimentary
MSI is that it does not allow updates to be installed on top of the existing
installation.  You must uninstall the old version before installing the newer
one.

The ``unix_packages`` directory contains the ``build_packages.sh`` script used
to generate the packages for the Curator YUM and APT repositories.  The
``Vagrant`` directory has the Vagrantfiles used in conjunction with the
``build_packages.sh`` script.  If you wish to use this method on your own, you
must ensure that the shared folders exist.  ``/curator_packages`` is where the
packages will be placed after building.  ``/curator_source`` is the path to the
Curator source code, so that the ``build_packages.sh`` script can be called from
there.  The ``build_packages.sh`` script does `not` use the local source code,
but rather pulls the version specified as an argument directly from GitHub.

Versioning
----------

Version 5 of Curator is the current ``master`` branch.  It supports only 5.x 
versions of Elasticsearch.


Origins
-------

Curator was first called ``clearESindices.py`` [1] and was almost immediately
renamed to ``logstash_index_cleaner.py`` [1].  After a time it was migrated under
the [logstash](https://github.com/elastic/logstash) repository as
``expire_logs``.  Soon thereafter, Jordan Sissel was hired by Elasticsearch, as
was the original author of this tool.  It became Elasticsearch Curator after
that and is now hosted at <https://github.com/elastic/curator>

[1] <https://logstash.jira.com/browse/LOGSTASH-211>
