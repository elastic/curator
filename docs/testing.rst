.. _testing:

Testing
#######

Ensuring that code changes work with new Elasticsearch versions, ``elasticsearch-py`` Python module
versions, and even new Python versions can be daunting. I've tried to make it easy to verify that
changes will work.

Setup Testing
*************

Since ``nose`` testing basically died somewhere during Curator's early days, a new testing
framework has become necessary. This is where ``pytest`` comes in.

Install ``pytest``
==================

From where your ``git`` cloned or forked repository is, you need to install Curator and its
dependencies, including for testing:

.. code-block:: shell

       pip install -U '.[test]'

Manually install testing dependencies
-------------------------------------

These are indicated in ``pyproject.toml`` in the ``[project.optional-dependencies]`` subsection.

An example is listed below:

.. code-block::

       [project.optional-dependencies]
       test = [
           "mock",
           "requests",
           "pytest >=7.2.1",
           "pytest-cov",
       ]

These may change with time, and this document not be updated, so double check dependencies here
before running the following:

.. code-block:: shell

       pip install -U mock requests pytest pytest-cov

It should be simpler to run the regular method, but if you have some reason to do this manually,
those are the steps.

Elasticsearch as a testing dependency
=====================================

.. warning::
    Not using a dedicated instance or instances for testing will result in deleted data!
    The tests perform setup and teardown functions which will delete anything in your cluster
    between each test.

.. important::
    Integration tests will at least require Elasticsearch running on ``http://127.0.0.1:9200`` or
    ``TEST_ES_SERVER`` being set. The few tests that require a remote cluster to be configured will
    need ``REMOTE_ES_SERVER`` to be set as well.

I will not cover how to install Elasticsearch locally in this document. It can be done, but it is
much easier to use Docker containers instead.

If you host a dedicated instance somewhere else (and it must be unsecured for testing), you can
specify this as an environment variable:

.. code-block:: shell

       TEST_ES_SERVER="http://10.0.0.1:9201" \
       pytest --cov=curator --cov-report html:cov_html

Additionally, four tests will be skipped if no value for ``REMOTE_ES_SERVER`` is provided.

.. code-block:: shell

       TEST_ES_SERVER="http://10.0.0.1:9201" \
       REMOTE_ES_SERVER="http://10.0.0.2:9201" \
       pytest --cov=curator --cov-report html:cov_html

The ``REMOTE_ES_SERVER`` must be a separate instance altogether, and the main instance must
whitelist that instance for reindexing operations. If that sounds complicated, you're not wrong.

There are remedies for this, and Curator comes with the necessary tools

Using Docker
------------

Fortunately, Curator provides an out-of-the-box, ready to go set of scripts for setting up not
only one container, but both containers necessary for testing the remote reindex functionality.

.. warning::
    Do not use anything but ``create.sh`` and ``destroy.sh``, or edit the ``Dockerfile.tmpl`` or
    ``small.options`` files unless you're actively trying to improve these scripts. These keep the
    Elasticsearch containers lean. Do examine ``create.sh`` to see which Elasticsearch startup
    flags are being used.

Create Docker containers for testing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Replace ``X.Y.Z`` with an Elasticsearch version:

.. code-block:: shell

        $ cd /path/to/curator_code/docker_test/scripts
        $ ./create.sh X.Y.Z
        Docker image curator_estest:8.6.1 not found. Building from Dockerfile...
        ...
        Waiting for Elasticsearch instances to become available...

This will create both Docker containers, and will print out the ``REMOTE_ES_SERVER`` line to use:

.. code-block:: shell

        Please select one of these environment variables to prepend your 'pytest' run:

        REMOTE_ES_SERVER="http://10.0.0.2:9201"

Clean up Docker containers used for testing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
    The container names ``curator8-es-local`` and ``curator8-es-remote`` are hard coded in both
    scripts so that ``destroy.sh`` will clean up exactly what ``create.sh`` made.

.. code-block:: shell

        $ cd /path/to/curator_code/docker_test/scripts
        $ ./destroy.sh
        curator8-es-local
        curator8-es-remote
        curator8-es-local
        curator8-es-remote
        Cleanup complete.

The ``repo`` directory
^^^^^^^^^^^^^^^^^^^^^^

``/path/to/curator_code/docker_test/repo`` will be created by ``create.sh`` and deleted by
``destroy.sh``. This is used for snapshot testing and will only ever contain a few files. Anything
snapshotted there temporarily is cleaned by the ``teardown`` between tests.

Running Tests
*************

Using ``pytest``
================

Using the value of ``REMOTE_ES_SERVER`` you got from ``create.sh``, or your own "remote"
Elasticsearch instance, testing is as simple as running:

.. note::
    All of these examples presume that you are at the base directory of Curator's code such that
    the ``tests`` direcory is visible.

.. code-block:: shell

       REMOTE_ES_SERVER="http://10.0.0.2:9201" pytest


Generating coverage reports
---------------------------

.. code-block:: shell

       $ REMOTE_ES_SERVER="http://10.0.0.2:9201" pytest --cov=curator
       ............................................................................ [ 12%]
       ............................................................................ [ 24%]
       ............................................................................ [ 36%]
       ............................................................................ [ 48%]
       ............................................................................ [ 60%]
       ............................................................................ [ 72%]
       ............................................................................ [ 84%]
       ............................................................................ [ 96%]
       ........................                                                     [100%]

       ---------- coverage: platform darwin, python 3.11.1-final-0 ----------
       Name                                     Stmts   Miss  Cover
       ------------------------------------------------------------
       curator/__init__.py                         10      0   100%
       curator/_version.py                          1      0   100%
       curator/actions/__init__.py                 14      0   100%
       ...
       curator/validators/schemacheck.py           42      0   100%
       ------------------------------------------------------------
       TOTAL                                     4023   1018    75%

       475 passed in 4.92s

Generating an HTML coverage report
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: shell

       $ REMOTE_ES_SERVER="http://10.0.0.2:9201" pytest --cov=curator --cov-reporthtml:cov_html
       ............................................................................ [ 12%]
       ............................................................................ [ 24%]
       ............................................................................ [ 36%]
       ............................................................................ [ 48%]
       ............................................................................ [ 60%]
       ............................................................................ [ 72%]
       ............................................................................ [ 84%]
       ............................................................................ [ 96%]
       ........................                                                     [100%]

       ---------- coverage: platform darwin, python 3.11.1-final-0 ----------
       Coverage HTML written to dir cov_html

       475 passed in 5.24s

At this point, you can view ``/path/to/curator_code/cov_html/index.html`` in your web browser. On
macOS, this is as simple as running:

.. code-block:: shell

       $ open cov_html.index.html

It will open in your default browser.

Testing only unit tests
-----------------------

As unit tests do not require a remote Elasticsearch instance, adding the ``REMOTE_ES_SERVER``
environment variable is unnecessary:

.. code-block:: shell

       $ pytest tests/unit

You can also add ``--cov=curator`` and/or ``--cov=curator html:cov_html`` options.

Testing only integration tests
------------------------------

Most integration tests do not require a remote Elasticsearch instance, so adding the
``REMOTE_ES_SERVER`` environment variable is unnecessary. Having a functional instance of
Elasticsearch at ``http://127.0.0.1:9200`` or the ``TEST_ES_SERVER`` environment variable set is
required.

.. code-block:: shell

       $ pytest tests/integration

You can also add ``--cov=curator`` and/or ``--cov=curator html:cov_html`` options.

This will result in 4 skipped tests:

.. code-block:: shell

       $ pytest tests/integration
       .......................................................................... [ 47%]
       ...............................s.s...ss................................... [ 94%]
       .........                                                                  [100%]
       ============================ short test summary info =============================
       SKIPPED [1] tests/integration/test_reindex.py:110: REMOTE_ES_SERVER is not defined
       SKIPPED [1] tests/integration/test_reindex.py:275: REMOTE_ES_SERVER is not defined
       SKIPPED [1] tests/integration/test_reindex.py:157: REMOTE_ES_SERVER is not defined
       SKIPPED [1] tests/integration/test_reindex.py:206: REMOTE_ES_SERVER is not defined
       153 passed, 4 skipped, 7 warnings in 217.76s (0:03:37)

You can see the ``s`` in the test output. The message for each skipped test also clearly explains
that ``REMOTE_ES_SERVER`` is undefined. If you were to run this with ``REMOTE_ES_SERVER``, it
would clear up the skipped tests.

Running specific tests
----------------------

These examples are all derived from unit tests, but the same formatting applies to integration
tests as well. The path for those will just be ``tests/integration/test_file.py``.

.. important::
    Integration tests will at least require Elasticsearch running on ``http://127.0.0.1:9200`` or
    ``TEST_ES_SERVER`` being set. The few tests that require a remote cluster to be configured will
    need ``REMOTE_ES_SERVER`` to be set as well.

Testing all tests within a given file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This will test every method of every class in ``test_file.py``

.. code-block:: shell

       $ pytest tests/unit/test_file.py
       ...................................................                        [100%]
       51 passed in 0.32s

Testing all tests within a given class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This will test every method of class ``TestClass`` in ``test_file.py``

.. code-block:: shell

       $ pytest tests/unit/test_file.py::TestClass
       ..............                                                             [100%]
       14 passed in 0.35s

Testing one test within a given class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This will test method ``test_method`` of class ``TestClass`` in ``test_file.py``

.. code-block:: shell

       $ pytest tests/unit/test_file.py::TestClass::test_method
       .                                                                          [100%]
       1 passed in 0.31s