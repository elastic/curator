.. _changelog:

Changelog
=========

4.0.6 (15 August 2016)
----------------------

**Bug Fixes**

  * Update old calls used with ES 1.x to reflect changes in 2.x+. This was
    necessary to work with Elasticsearch 5.0.0-alpha5. Fixed in #728 (untergeek)

**Doc Fixes**

  * Add section detailing that the value of a ``value`` filter element should be
    encapsulated in single quotes. Reported in #726. (untergeek)

4.0.5 (3 August 2016)
---------------------

**Bug Fixes**

  * Fix incorrect variable name for AWS Region reported in #679 (basex)
  * Fix ``filter_by_space()`` to not fail when index age metadata is not
    present.  Indices without the appropriate age metadata will instead be
    excluded, with a debug-level message. Reported in #724 (untergeek)

**Doc Fixes**

  * Fix documentation for the space filter and the source filter element.

4.0.4 (1 August 2016)
---------------------

**Bug Fixes**

  * Fix incorrect variable name in Allocation action. #706 (lukewaite)
  * Incorrect error message in ``create_snapshot_body`` reported in #711
    (untergeek)
  * Test for empty index list object should happen in action initialization for
    snapshot action. Discovered in #711. (untergeek)

**Doc Fixes**

  * Add menus to asciidoc chapters #704 (untergeek)
  * Add pyyaml dependency #710 (dtrv)

4.0.3 (22 July 2016)
--------------------

**General**

  * 4.0.2 didn't work for ``pip`` installs due to an omission in the
    MANIFEST.in file.  This came up during release testing, but before the
    release was fully published. As the release was never fully published, this
    should not have actually affected anyone.

**Bug Fixes**

  * These are the same as 4.0.2, but it was never fully released.
  * All default settings are now values returned from functions instead of
    constants.  This was resulting in settings getting stomped on. New test
    addresses the original complaint.  This removes the need for ``deepcopy``.
    See issue #687 (untergeek)
  * Fix ``host`` vs. ``hosts`` issue in ``get_client()`` rather than the
    non-functional function in ``repomgrcli.py``.
  * Update versions being tested.
  * Community contributed doc fixes.
  * Reduced logging verbosity by making most messages debug level. #684
    (untergeek)
  * Fixed log whitelist behavior (and switched to blacklisting instead).
    Default behavior will now filter traffic from the ``elasticsearch`` and
    ``urllib3`` modules.
  * Fix Travis CI testing to accept some skipped tests, as needed. #695
    (untergeek)
  * Fix missing empty index test in snapshot action. #682 (sherzberg)

4.0.2 (22 July 2016)
--------------------

**Bug Fixes**

  * All default settings are now values returned from functions instead of
    constants.  This was resulting in settings getting stomped on. New test
    addresses the original complaint.  This removes the need for ``deepcopy``.
    See issue #687 (untergeek)
  * Fix ``host`` vs. ``hosts`` issue in ``get_client()`` rather than the
    non-functional function in ``repomgrcli.py``.
  * Update versions being tested.
  * Community contributed doc fixes.
  * Reduced logging verbosity by making most messages debug level. #684
    (untergeek)
  * Fixed log whitelist behavior (and switched to blacklisting instead).
    Default behavior will now filter traffic from the ``elasticsearch`` and
    ``urllib3`` modules.
  * Fix Travis CI testing to accept some skipped tests, as needed. #695
    (untergeek)
  * Fix missing empty index test in snapshot action. #682 (sherzberg)

4.0.1 (1 July 2016)
-------------------

**Bug Fixes**

  * Coerce Logstash/JSON logformat type timestamp value to always use UTC.
    #661 (untergeek)
  * Catch and remove indices from the actionable list if they do not have a
    `creation_date` field in settings.  This field was introduced in ES v1.4, so
    that indicates a rather old index. #663 (untergeek)
  * Replace missing ``state`` filter for ``snapshotlist``. #665 (untergeek)
  * Restore ``es_repo_mgr`` as a stopgap until other CLI scripts are added.  It
    will remain undocumented for now, as I am debating whether to make
    repository creation its own action in the API. #668 (untergeek)
  * Fix dry run results for snapshot action. #673 (untergeek)

4.0.0 (24 June 2016)
--------------------

It's official!  Curator 4.0.0 is released!

**Breaking Changes**

  * New and improved API!
  * Command-line changes.  No more command-line args, except for ``--config``,
    ``--actions``, and ``--dry-run``:

      - ``--config`` points to a YAML client and logging configuration file.
        The default location is ``~/.curator/curator.yml``
      - ``--actions`` arg points to a YAML action configuration file
      - ``--dry-run`` will simulate the action(s) which would have taken place,
        but not actually make any changes to the cluster or its indices.

**New Features**

  * Snapshot restore is here!
  * YAML configuration files.  Now a single file can define an entire batch of
    commands, each with their own filters, to be performed in sequence.
  * Sort by index age not only by index name (as with previous versions of
    Curator), but also by index `creation_date`, or by calculations from the
    Field Stats API on a timestamp field.
  * Atomically add/remove indices from aliases! This is possible by way of the
    new `IndexList` class and YAML configuration files.
  * State of indices pulled and stored in `IndexList` instance.  Fewer API calls
    required to serially test for open/close, `size_in_bytes`, etc.
  * Filter by space now allows sorting by age!
  * Experimental! Use AWS IAM credentials to sign requests to Elasticsearch.
    This requires the end user to *manually* install the `requests_aws4auth`
    python module.
  * Optionally delete aliases from indices before closing.
  * An empty index or snapshot list no longer results in an error if you set
    ``ignore_empty_list`` to `True`.  If `True` it will still log that the
    action was not performed, but will continue to the next action. If 'False'
    it will log an ERROR and exit with code 1.

**API**

  * Updated API documentation
  * Class: `IndexList`. This pulls all indices at instantiation, and you apply
    filters, which are class methods.  You can iterate over as many filters as
    you like, in fact, due to the YAML config file.
  * Class: `SnapshotList`. This pulls all snapshots from the given repository at
    instantiation, and you apply filters, which are class methods.  You can
    iterate over as many filters as you like, in fact, due to the YAML config
    file.
  * Add `wait_for_completion` to Allocation and Replicas actions.  These will
    use the client timeout, as set by default or `timeout_override`, to
    determine how long to wait for timeout.  These are handled in batches of
    indices for now.
  * Allow `timeout_override` option for all actions.  This allows for different
    timeout values per action.
  * Improve API by giving each action its own `do_dry_run()` method.

**General**

  * Updated use documentation for Elastic main site.
  * Include example files for ``--config`` and ``--actions``.

4.0.0b2 (16 June 2016)
----------------------

**Second beta release of the 4.0 branch**

**New Feature**

  * An empty index or snapshot list no longer results in an error if you set
    ``ignore_empty_list`` to `True`.  If `True` it will still log that the
    action was not performed, but will continue to the next action. If 'False'
    it will log an ERROR and exit with code 1. (untergeek)

4.0.0b1 (13 June 2016)
----------------------

**First beta release of the 4.0 branch!**

The release notes will be rehashing the new features in 4.0, rather than the
bug fixes done during the alphas.

**Breaking Changes**

  * New and improved API!
  * Command-line changes.  No more command-line args, except for ``--config``,
    ``--actions``, and ``--dry-run``:

      - ``--config`` points to a YAML client and logging configuration file.
        The default location is ``~/.curator/curator.yml``
      - ``--actions`` arg points to a YAML action configuration file
      - ``--dry-run`` will simulate the action(s) which would have taken place,
        but not actually make any changes to the cluster or its indices.

**New Features**

  * Snapshot restore is here!
  * YAML configuration files.  Now a single file can define an entire batch of
    commands, each with their own filters, to be performed in sequence.
  * Sort by index age not only by index name (as with previous versions of
    Curator), but also by index `creation_date`, or by calculations from the
    Field Stats API on a timestamp field.
  * Atomically add/remove indices from aliases! This is possible by way of the
    new `IndexList` class and YAML configuration files.
  * State of indices pulled and stored in `IndexList` instance.  Fewer API calls
    required to serially test for open/close, `size_in_bytes`, etc.
  * Filter by space now allows sorting by age!
  * Experimental! Use AWS IAM credentials to sign requests to Elasticsearch.
    This requires the end user to *manually* install the `requests_aws4auth`
    python module.
  * Optionally delete aliases from indices before closing.

**API**

  * Updated API documentation
  * Class: `IndexList`. This pulls all indices at instantiation, and you apply
    filters, which are class methods.  You can iterate over as many filters as
    you like, in fact, due to the YAML config file.
  * Class: `SnapshotList`. This pulls all snapshots from the given repository at
    instantiation, and you apply filters, which are class methods.  You can
    iterate over as many filters as you like, in fact, due to the YAML config
    file.
  * Add `wait_for_completion` to Allocation and Replicas actions.  These will
    use the client timeout, as set by default or `timeout_override`, to
    determine how long to wait for timeout.  These are handled in batches of
    indices for now.
  * Allow `timeout_override` option for all actions.  This allows for different
    timeout values per action.
  * Improve API by giving each action its own `do_dry_run()` method.

**General**

  * Updated use documentation for Elastic main site.
  * Include example files for ``--config`` and ``--actions``.


4.0.0a10 (10 June 2016)
-----------------------

**New Features**

  * Snapshot restore is here!
  * Optionally delete aliases from indices before closing. Fixes #644 (untergeek)

**General**

  * Add `wait_for_completion` to Allocation and Replicas actions.  These will
    use the client timeout, as set by default or `timeout_override`, to
    determine how long to wait for timeout.  These are handled in batches of
    indices for now.
  * Allow `timeout_override` option for all actions.  This allows for different
    timeout values per action.

**Bug Fixes**

  * Disallow use of `master_only` if multiple hosts are used. Fixes #615
    (untergeek)
  * Fix an issue where arguments weren't being properly passed and populated.
  * ForceMerge replaced Optimize in ES 2.1.0.
  * Fix prune_nones to work with Python 2.6. Fixes #619 (untergeek)
  * Fix TimestringSearch to work with Python 2.6. Fixes #622 (untergeek)
  * Add language classifiers to ``setup.py``.  Fixes #640 (untergeek)
  * Changed references to readthedocs.org to be readthedocs.io.

4.0.0a9 (27 Apr 2016)
---------------------

**General**

  * Changed `create_index` API to use kwarg `extra_settings` instead of `body`
  * Normalized Alias action to use `name` instead of `alias`.  This simplifies
    documentation by reducing the number of option elements.
  * Streamlined some code
  * Made `exclude` a filter element setting for all filters. Updated all
    examples to show this.
  * Improved documentation

**New Features**

  * Alias action can now accept `extra_settings` to allow adding filters, and/or
    routing.


4.0.0a8 (26 Apr 2016)
---------------------

**Bug Fixes**

  * Fix to use `optimize` with versions of Elasticsearch < 5.0
  * Fix missing setting in testvars


4.0.0a7 (25 Apr 2016)
---------------------

**Bug Fixes**

  * Fix AWS4Auth error.

4.0.0a6 (25 Apr 2016)
---------------------

**General**

  * Documentation updates.
  * Improve API by giving each action its own `do_dry_run()` method.

**Bug Fixes**

  * Do not escape characters other than ``.`` and ``-`` in timestrings. Fixes
    #602 (untergeek)

** New Features**

  * Added `CreateIndex` action.

4.0.0a4 (21 Apr 2016)
---------------------

**Bug Fixes**

  * Require `pyyaml` 3.10 or better.
  * In the case that no `options` are in an action, apply the defaults.

4.0.0a3 (21 Apr 2016)
---------------------

It's time for Curator 4.0 alpha!

**Breaking Changes**

  * New API! (again?!)
  * Command-line changes.  No more command-line args, except for ``--config``,
    ``--actions``, and ``--dry-run``:

      - ``--config`` points to a YAML client and logging configuration file.
        The default location is ``~/.curator/curator.yml``
      - ``--actions`` arg points to a YAML action configuration file
      - ``--dry-run`` will simulate the action(s) which would have taken place,
        but not actually make any changes to the cluster or its indices.

**General**

  * Updated API documentation
  * Updated use documentation for Elastic main site.
  * Include example files for ``--config`` and ``--actions``.

**New Features**

  * Sort by index age not only by index name (as with previous versions of
    Curator), but also by index `creation_date`, or by calculations from the
    Field Stats API on a timestamp field.
  * Class: `IndexList`. This pulls all indices at instantiation, and you apply
    filters, which are class methods.  You can iterate over as many filters as
    you like, in fact, due to the YAML config file.
  * Class: `SnapshotList`. This pulls all snapshots from the given repository at
    instantiation, and you apply filters, which are class methods.  You can
    iterate over as many filters as you like, in fact, due to the YAML config
    file.
  * YAML configuration files.  Now a single file can define an entire batch of
    commands, each with their own filters, to be performed in sequence.
  * Atomically add/remove indices from aliases! This is possible by way of the
    new `IndexList` class and YAML configuration files.
  * State of indices pulled and stored in `IndexList` instance.  Fewer API calls
    required to serially test for open/close, `size_in_bytes`, etc.
  * Filter by space now allows sorting by age!
  * Experimental! Use AWS IAM credentials to sign requests to Elasticsearch.
    This requires the end user to *manually* install the `requests_aws4auth`
    python module.

3.5.1 (21 March 2016)
---------------------

**Bug fixes**

  * Add more logging information to snapshot delete method #582 (untergeek)
  * Improve default timeout, logging, and exception handling for `seal` command
    #583 (untergeek)
  * Fix use of default snapshot name. #584 (untergeek)


3.5.0 (16 March 2016)
---------------------

**General**

  * Add support for the `--client-cert` and `--client-key` command line parameters
    and client_cert and client_key parameters to the get_client() call. #520 (richm)

**Bug fixes**

  * Disallow users from creating snapshots with upper-case letters, which is not
    permitted by Elasticsearch. #562 (untergeek)
  * Remove `print()` command from ``setup.py`` as it causes issues with command-
    line retrieval of ``--url``, etc. #568 (thib-ack)
  * Remove unnecessary argument from `build_filter()` #530 (zzugg)
  * Allow day of year filter to be made up with 1, 2 or 3 digits #578 (petitout)


3.4.1 (10 February 2016)
------------------------

**General**

  * Update license copyright to 2016
  * Use slim python version with Docker #527 (xaka)
  * Changed ``--master-only`` exit code to 0 when connected to non-master node #540 (wkruse)
  * Add ``cx_Freeze`` capability to ``setup.py``, plus a ``binary_release.py``
    script to simplify binary package creation.  #554 (untergeek)
  * Set Elastic as author. #555 (untergeek)
  * Put repository creation methods into API and document them. Requested in #550 (untergeek)

**Bug fixes**

  * Fix sphinx documentation build error #506 (hydrapolic)
  * Ensure snapshots are found before iterating #507 (garyelephant)
  * Fix a doc inconsistency #509 (pmoust)
  * Fix a typo in `show` documentation #513 (pbamba)
  * Default to trying the cluster state for checking whether indices are closed, and
    then fall back to using the _cat API (for Amazon ES instances). #519 (untergeek)
  * Improve logging to show time delay between optimize runs, if selected. #525 (untergeek)
  * Allow elasticsearch-py module versions through 2.3.0 (a presumption at this point) #524 (untergeek)
  * Improve logging in snapshot api method to reveal when a repository appears to be
    missing. Reported in #551 (untergeek)
  * Test that ``--timestring`` has the correct variable for ``--time-unit``.
    Reported in #544 (untergeek)
  * Allocation will exit with exit_code 0 now when there are no indices to work on.
    Reported in #531 (untergeek)


3.4.0 (28 October 2015)
-----------------------

**General**

  * API change in elasticsearch-py 1.7.0 prevented alias operations.  Fixed in
    #486 (HonzaKral)
  * During index selection you can now select only closed indices with ``--closed-only``.
    Does not impact ``--all-indices`` Reported in #476. Fixed in #487 (Basster)
  * API Changes in Elasticsearch 2.0.0 required some refactoring.  All tests pass
    for ES versions 1.0.3 through 2.0.0-rc1.  Fixed in #488 (untergeek)
  * es_repo_mgr now has access to the same SSL options from #462. #489 (untergeek)
  * Logging improvements requested in #475. (untergeek)
  * Added ``--quiet`` flag. #494 (untergeek)
  * Fixed ``index_closed`` to work with AWS Elasticsearch. #499 (univerio)
  * Acceptable versions of Elasticsearch-py module are 1.8.0 up to 2.1.0 (untergeek)

3.3.0 (31 August 2015)
----------------------

**Announcement**

  * Curator is tested in Jenkins.  Each commit to the master branch is tested
    with both Python versions 2.7.6 and 3.4.0 against each of the following
    Elasticsearch versions:
    * 1.7_nightly
    * 1.6_nightly
    * 1.7.0
    * 1.6.1
    * 1.5.1
    * 1.4.4
    * 1.3.9
    * 1.2.4
    * 1.1.2
    * 1.0.3
  * If you are using a version different from this, your results may vary.

**General**

  * Allocation type can now also be ``include`` or ``exclude``, in addition to the
    the existing default ``require`` type. Add ``--type`` to the allocation command
    to specify the type. #443 (steffo)

  * Bump elasticsearch python module dependency to 1.6.0+ to enable synced_flush
    API call. Reported in #447 (untergeek)

  * Add SSL features, ``--ssl-no-validate`` and ``certificate`` to provide other
    ways to validate SSL connections to Elasticsearch. #436 (untergeek)

**Bug fixes**

  * Delete by space was only reporting space used by primary shards.  Fixed to
    show all space consumed.  Reported in #455 (untergeek)

  * Update exit codes and messages for snapshot selection.  Reported in #452 (untergeek)

  * Fix potential int/float casting issues. Reported in #465 (untergeek)

3.2.3 (16 July 2015)
--------------------

**Bug fix**

  * In order to address customer and community issues with bulk deletes, the
    ``master_timeout`` is now invoked for delete operations.  This should address
    503s with 30s timeouts in the debug log, even when ``--timeout`` is set to
    a much higher value.  The ``master_timeout`` is tied to the ``--timeout``
    flag value, but will not exceed 300 seconds. #420 (untergeek)

**General**

  * Mixing it up a bit here by putting `General` second!  The only other changes
    are that logging has been improved for deletes so you won't need to have the
    ``--debug`` flag to see if you have error codes >= 400, and some code
    documentation improvements.

3.2.2 (13 July 2015)
--------------------

**General**

  * This is a very minor change.  The ``mock`` library recently removed support
    for Python 2.6.  As many Curator users are using RHEL/CentOS 6, which is
    pinned to Python 2.6, this requires the mock version referenced by Curator
    to also be pinned to a supported version (``mock==1.0.1``).

3.2.1 (10 July 2015)
--------------------

**General**

  * Added delete verification & retry (fixed at 3x) to potentially cover an edge
    case in #420 (untergeek)
  * Since GitHub allows rST (reStructuredText) README documents, and that's what
    PyPI wants also, the README has been rebuilt in rST. (untergeek)

**Bug fixes**

  * If closing indices with ES 1.6+, and all indices are closed, ensure that the
    seal command does not try to seal all indices.  Reported in #426 (untergeek)
  * Capture AttributeError when sealing indices if a non-TransportError occurs.
    Reported in #429 (untergeek)

3.2.0 (25 June 2015)
--------------------

**New!**

  * Added support to manually seal, or perform a [synced flush](http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-synced-flush.html)
    on indices with the ``seal`` command. #394 (untergeek)
  * Added *experimental* support for SSL certificate validation.  In order for
    this to work, you must install the ``certifi`` python module:
    ``pip install certifi``
    This feature *should* automatically work if the ``certifi`` module is
    installed.  Please report any issues.

**General**

  * Changed logging to go to stdout rather than stderr.  Reopened #121 and
    figured they were right.  This is better. (untergeek)
  * Exit code 99 was unpopular.  It has been removed. Reported in #371 and #391
    (untergeek)
  * Add ``--skip-repo-validation`` flag for snapshots.  Do not validate write
    access to repository on all cluster nodes before proceeding. Useful for
    shared filesystems where intermittent timeouts can affect validation, but
    won't likely affect snapshot success. Requested in #396 (untergeek)
  * An alias no longer needs to be pre-existent in order to use the alias
    command.  #317 (untergeek)
  * es_repo_mgr now passes through upstream errors in the event a repository
    fails to be created.  Requested in #405 (untergeek)

**Bug fixes**

 * In rare cases, ``*`` wildcard would not expand.  Replaced with _all.
   Reported in #399 (untergeek)
 * Beginning with Elasticsearch 1.6, closed indices cannot have their replica
   count altered.  Attempting to do so results in this error:
   ``org.elasticsearch.ElasticsearchIllegalArgumentException: Can't update [index.number_of_replicas] on closed indices [[test_index]] - can leave index in an unopenable state``
   As a result, the ``change_replicas`` method has been updated to prune closed
   indices.  This change will apply to all versions of Elasticsearch.
   Reported in #400 (untergeek)
 * Fixed es_repo_mgr repository creation verification error. Reported in #389
   (untergeek)



3.1.0 (21 May 2015)
-------------------

**General**

 * If ``wait_for_completion`` is true, snapshot success is now tested and logged.
   Reported in #253 (untergeek)
 * Log & return false if a snapshot is already in progress (untergeek)
 * Logs individual deletes per index, even though they happen in batch mode.
   Also log individual snapshot deletions. Reported in #372 (untergeek)
 * Moved ``chunk_index_list`` from cli to api utils as it's now also used by ``filter.py``
 * Added a warning and 10 second timer countdown if you use ``--timestring`` to filter
   indices, but do not use ``--older-than`` or ``--newer-than`` in conjunction with it.
   This is to address #348, which behavior isn't a bug, but prevents accidental
   action against all of your time-series indices.  The warning and timer are
   not displayed for ``show`` and ``--dry-run`` operations.
 * Added tests for ``es_repo_mgr`` in #350
 * Doc fixes

**Bug fixes**

 * delete-by-space needed the same fix used for #245. Fixed in #353 (untergeek)
 * Increase default client timeout for ``es_repo_mgr`` as node discovery and
   availability checks for S3 repositories can take a bit.  Fixed in #352 (untergeek)
 * If an index is closed, indicate in ``show`` and ``--dry-run`` output.
   Reported in #327. (untergeek)
 * Fix issue where CLI parameters were not being passed to the ``es_repo_mgr``
   create sub-command.
   Reported in #337. (feltnerm)

3.0.3 (27 Mar 2015)
-------------------

**Announcement**

This is a bug fix release. #319 and #320 are affecting a few users, so this
release is being expedited.

Test count: 228
Code coverage: 99%

**General**

 * Documentation for the CLI converted to Asciidoc and moved to
   http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html
 * Improved logging, and refactored a few methods to help with this.
 * Dry-run output is now more like v2, with the index or snapshot in the log
   line, along with the command.  Several tests needed refactoring with this
   change, along with a bit of documentation.

**Bug fixes**

 * Fix links to repository in setup.py. Reported in #318 (untergeek)
 * No more ``--delay`` with optimized indices. Reported in #319 (untergeek)
 * ``--request_timeout`` not working as expected.  Reinstate the version 2
   timeout override feature to prevent default timeouts for ``optimize`` and
   ``snapshot`` operations. Reported in #320 (untergeek)
 * Reduce index count to 200 for
   test.integration.test_cli_commands.TestCLISnapshot.test_cli_snapshot_huge_list
   in order to reduce or eliminate Jenkins CI test timeouts.
   Reported in #324 (untergeek)
 * ``--dry-run`` no longer calls ``show``, but will show output in the log, as
   in v2. This was a recurring complaint.  See #328 (untergeek)


3.0.2 (23 Mar 2015)
-------------------

**Announcement**

This is a bug fix release.  #307 and #309 were big enough to warrant an
expedited release.

**Bug fixes**

 * Purge unneeded constants, and clean up config options for snapshot. Reported in #303 (untergeek)
 * Don't split large index list if performing snapshots. Reported in #307 (untergeek)
 * Act correctly if a zero value for `--older-than` or `--newer-than` is provided. #309 (untergeek)

3.0.1 (16 Mar 2015)
-------------------

**Announcement**

The ``regex_iterate`` method was horribly named.  It has been renamed to
``apply_filter``.  Methods have been added to allow API users to build a
filtered list of indices similarly to how the CLI does.  This was an oversight.
Props to @SegFaultAX for pointing this out.

**General**

 * In conjunction with the rebrand to Elastic, URLs and documentation were updated.
 * Renamed horribly named `regex_iterate` method to `apply_filter` #298 (untergeek)
 * Added `build_filter` method to mimic CLI calls. #298 (untergeek)
 * Added Examples page in the API documentation. #298 (untergeek)

**Bug fixes**

 * Refactored to show `--dry-run` info for `--disk-space` calls. Reported in
   #290 (untergeek)
 * Added list chunking so acting on huge lists of indices won't result in a URL
   bigger than 4096 bytes (Elasticsearch's default limit.)  Reported in
   https://github.com/elastic/curator/issues/245#issuecomment-77916081
 * Refactored `to_csv()` method to be simpler.
 * Added and removed tests according to changes.  Code coverage still at 99%

3.0.0 (9 March 2015)
--------------------

**Release Notes**

The full release of Curator 3.0 is out!  Check out all of the changes here!

*Note:* This release is _not_ reverse compatible with any previous version.

Because 3.0 is a major point release, there have been some major changes to both
the API as well as the CLI arguments and structure.

Be sure to read the updated command-line specific docs in the
[wiki](https://github.com/elasticsearch/curator/wiki) and change your
command-line arguments accordingly.

The API docs are still at http://curator.readthedocs.io.  Be sure to read the
latest docs, or select the docs for 3.0.0.

**General**

 * **Breaking changes to the API.**  Because this is a major point revision,
   changes to the API have been made which are non-reverse compatible.  Before
   upgrading, be sure to update your scripts and test them thoroughly.
 * **Python 3 support** Somewhere along the line, Curator would no longer work
   with curator.  All tests now pass for both Python2 and Python3, with 99% code
   coverage in both environments.
 * **New CLI library.** Using Click now. http://click.pocoo.org/3/
   This change is especially important as it allows very easy CLI integration
   testing.
 * **Pipelined filtering!** You can now use ``--older-than`` & ``--newer-than``
   in the same command!  You can also provide your own regex via the ``--regex``
   parameter.  You can use multiple instances of the ``--exclude`` flag.
 * **Manually include indices!** With the ``--index`` paramter, you can add an
   index to the working list.  You can provide multiple instances of the
   ``--index`` parameter as well!
 * **Tests!** So many tests now.  Test coverage of the API methods is at 100%
   now, and at 99% for the CLI methods.  This doesn't mean that all of the tests
   are perfect, or that I haven't missed some scenarios.  It does mean, however,
   that it will be much easier to write tests if something turns up missed.  It
   also means that any new functionality will now need to have tests.
 * **Iteration changes** Methods now only iterate through each index when
   appropriate!  In fact, the only commands that iterate are `alias` and
   `optimize`.  The `bloom` command will iterate, but only if you have added the
   `--delay` flag with a value greater than zero.
 * **Improved packaging!**  Methods have been moved into categories of
   ``api`` and ``cli``, and further broken out into individual modules to help
   them be easier to find and read.
 * Check for allocation before potentially re-applying an allocation rule.
   #273 (ferki)
 * Assigning replica count and routing allocation rules _can_ be done to closed
   indices. #283 (ferki)

**Bug fixes**

 * Don't accidentally delete ``.kibana`` index. #261 (malagoli)
 * Fix segment count for empty indices. #265 (untergeek)
 * Change bloom filter cutoff Elasticsearch version to 1.4. Reported in #267
   (untergeek)

3.0.0rc1 (5 March 2015)
-----------------------

**Release Notes**

RC1 is here!  I'm re-releasing the Changes from all betas here, minus the
intra-beta code fixes.  Barring any show stoppers, the official release will be
soon.

**General**

 * **Breaking changes to the API.**  Because this is a major point revision,
   changes to the API have been made which are non-reverse compatible.  Before
   upgrading, be sure to update your scripts and test them thoroughly.
 * **Python 3 support** Somewhere along the line, Curator would no longer work
   with curator.  All tests now pass for both Python2 and Python3, with 99% code
   coverage in both environments.
 * **New CLI library.** Using Click now. http://click.pocoo.org/3/
   This change is especially important as it allows very easy CLI integration
   testing.
 * **Pipelined filtering!** You can now use ``--older-than`` & ``--newer-than``
   in the same command!  You can also provide your own regex via the ``--regex``
   parameter.  You can use multiple instances of the ``--exclude`` flag.
 * **Manually include indices!** With the ``--index`` paramter, you can add an
   index to the working list.  You can provide multiple instances of the
   ``--index`` parameter as well!
 * **Tests!** So many tests now.  Test coverage of the API methods is at 100%
   now, and at 99% for the CLI methods.  This doesn't mean that all of the tests
   are perfect, or that I haven't missed some scenarios.  It does mean, however,
   that it will be much easier to write tests if something turns up missed.  It
   also means that any new functionality will now need to have tests.
 * Methods now only iterate through each index when appropriate!
 * Improved packaging!  Hopefully the ``entry_point`` issues some users have had
   will be addressed by this.  Methods have been moved into categories of
   ``api`` and ``cli``, and further broken out into individual modules to help
   them be easier to find and read.
 * Check for allocation before potentially re-applying an allocation rule.
   #273 (ferki)
 * Assigning replica count and routing allocation rules _can_ be done to closed
   indices. #283 (ferki)

**Bug fixes**

 * Don't accidentally delete ``.kibana`` index. #261 (malagoli)
 * Fix segment count for empty indices. #265 (untergeek)
 * Change bloom filter cutoff Elasticsearch version to 1.4. Reported in #267
   (untergeek)


3.0.0b4 (5 March 2015)
----------------------

**Notes**

Integration testing!  Because I finally figured out how to use the Click
Testing API, I now have a good collection of command-line simulations,
complete with a real back-end.  This testing found a few bugs (this is why
testing exists, right?), and fixed a few of them.

**Bug fixes**

 * HUGE! `curator show snapshots` would _delete_ snapshots.  This is fixed.
 * Return values are now being sent from the commands.
 * `scripttest` is no longer necessary (click.Test works!)
 * Calling `get_snapshot` without a snapshot name returns all snapshots


3.0.0b3 (4 March 2015)
----------------------

**Bug fixes**

 * setup.py was lacking the new packages "curator.api" and "curator.cli"  The
   package works now.
 * Python3 suggested I had to normalize the beta tag to just b3, so that's also
   changed.
 * Cleaned out superfluous imports and logger references from the __init__.py
   files.

3.0.0-beta2 (3 March 2015)
--------------------------

**Bug fixes**

 * Python3 issues resolved.  Tests now pass on both Python2 and Python3

3.0.0-beta1 (3 March 2015)
--------------------------

**General**

 * **Breaking changes to the API.**  Because this is a major point revision,
   changes to the API have been made which are non-reverse compatible.  Before
   upgrading, be sure to update your scripts and test them thoroughly.
 * **New CLI library.** Using Click now. http://click.pocoo.org/3/
 * **Pipelined filtering!** You can now use ``--older-than`` & ``--newer-than``
   in the same command!  You can also provide your own regex via the ``--regex``
   parameter.  You can use multiple instances of the ``--exclude`` flag.
 * **Manually include indices!** With the ``--index`` paramter, you can add an
   index to the working list.  You can provide multiple instances of the
   ``--index`` parameter as well!
 * **Tests!** So many tests now.  Unit test coverage of the API methods is at
   100% now.  This doesn't mean that all of the tests are perfect, or that I
   haven't missed some scenarios.  It does mean that any new functionality will
   need to also have tests, now.
 * Methods now only iterate through each index when appropriate!
 * Improved packaging!  Hopefully the ``entry_point`` issues some users have had
   will be addressed by this.  Methods have been moved into categories of
   ``api`` and ``cli``, and further broken out into individual modules to help
   them be easier to find and read.
 * Check for allocation before potentially re-applying an allocation rule.
   #273 (ferki)

**Bug fixes**

 * Don't accidentally delete ``.kibana`` index. #261 (malagoli)
 * Fix segment count for empty indices. #265 (untergeek)
 * Change bloom filter cutoff Elasticsearch version to 1.4. Reported in #267 (untergeek)


2.1.2 (22 January 2015)
-----------------------

**Bug fixes**

 * Do not try to set replica count if count matches provided argument. #247 (bobrik)
 * Fix JSON logging (Logstash format). #250 (magnusbaeck)
 * Fix bug in `filter_by_space()` which would match all indices if the provided patterns found no matches. Reported in #254 (untergeek)

2.1.1 (30 December 2014)
------------------------

**Bug fixes**

 * Renamed unnecessarily redundant ``--replicas`` to ``--count`` in args for ``curator_script.py``

2.1.0 (30 December 2014)
------------------------

**General**

 * Snapshot name now appears in log output or STDOUT. #178 (untergeek)
 * Replicas! You can now change the replica count of indices. Requested in #175 (untergeek)
 * Delay option added to Bloom Filter functionality. #206 (untergeek)
 * Add 2-digit years as acceptable pattern (y vs. Y). Reported in #209 (untergeek)
 * Add Docker container definition #226 (christianvozar)
 * Allow the use of 0 with --older-than, --most-recent and --delete-older-than. See #208. #211 (bobrik)

**Bug fixes**

 * Edge case where 1.4.0.Beta1-SNAPSHOT would break version check. Reported in #183 (untergeek)
 * Typo fixed. #193 (ferki)
 * Type fixed. #204 (gheppner)
 * Shows proper error in the event of concurrent snapshots. #177 (untergeek)
 * Fixes erroneous index display of ``_, a, l, l`` when --all-indices selected. Reported in #222 (untergeek)
 * Use json.dumps() to escape exceptions. Reported in #210 (untergeek)
 * Check if index is closed before adding to alias.  Reported in #214 (bt5e)
 * No longer force-install argparse if pre-installed #216 (whyscream)
 * Bloom filters have been removed from Elasticsearch 1.5.0. Update methods and tests to act accordingly. #233 (untergeek)

2.0.2 (8 October 2014)
----------------------

**Bug fixes**

 * Snapshot name not displayed in log or STDOUT #185 (untergeek)
 * Variable name collision in delete_snapshot() #186 (untergeek)

2.0.1 (1 October 2014)
----------------------

**Bug fix**

 * Override default timeout when snapshotting --all-indices #179 (untergeek)

2.0.0 (25 September 2014)
-------------------------

**General**

 * New! Separation of Elasticsearch Curator Python API and curator_script.py (untergeek)
 * New! ``--delay`` after optimize to allow cluster to quiesce #131 (untergeek)
 * New! ``--suffix`` option in addition to ``--prefix`` #136 (untergeek)
 * New! Support for wildcards in prefix & suffix #136 (untergeek)
 * Complete refactor of snapshots.  Now supporting incrementals! (untergeek)

**Bug fix**

 * Incorrect error msg if no indices sent to create_snapshot (untergeek)
 * Correct for API change coming in ES 1.4 #168 (untergeek)
 * Missing ``"`` in Logstash log format #143 (cassianoleal)
 * Change non-master node test to exit code 0, log as ``INFO``. #145 (untergeek)
 * `months` option missing from validate_timestring() (untergeek)

1.2.2 (29 July 2014)
--------------------

**Bug fix**

 * Updated ``README.md`` to briefly explain what curator does #117 (untergeek)
 * Fixed ``es_repo_mgr`` logging whitelist #119 (untergeek)
 * Fixed absent ``months`` time-unit #120 (untergeek)
 * Filter out ``.marvel-kibana`` when prefix is ``.marvel-`` #120 (untergeek)
 * Clean up arg parsing code where redundancy exists #123 (untergeek)
 * Properly divide debug from non-debug logging #125 (untergeek)
 * Fixed ``show`` command bug caused by changes to command structure #126 (michaelweiser)

1.2.1 (24 July 2014)
--------------------

**Bug fix**

 * Fixed the new logging when called by ``curator`` entrypoint.

1.2.0 (24 July 2014)
--------------------

**General**

 * New! Allow user-specified date patterns: ``--timestring`` #111 (untergeek)
 * New! Curate weekly indices (must use week of year) #111 (untergeek)
 * New! Log output in logstash format ``--logformat logstash`` #111 (untergeek)
 * Updated! Cleaner default logs (debug still shows everything) (untergeek)
 * Improved! Dry runs are more visible in log output (untergeek)

Errata

 * The ``--separator`` option was removed in lieu of user-specified date patterns.
 * Default ``--timestring`` for days: ``%Y.%m.%d`` (Same as before)
 * Default ``--timestring`` for hours: ``%Y.%m.%d.%H`` (Same as before)
 * Default ``--timestring`` for weeks: ``%Y.%W``

1.1.3 (18 July 2014)
--------------------

**Bug fix**

 * Prefix not passed in ``get_object_list()`` #106 (untergeek)
 * Use ``os.devnull`` instead of ``/dev/null`` for Windows #102 (untergeek)
 * The http auth feature was erroneously omitted #100 (bbuchacher)

1.1.2 (13 June 2014)
--------------------

**Bug fix**

 * This was a showstopper bug for anyone using RHEL/CentOS with a Python 2.6 dependency for yum
 * Python 2.6 does not like format calls without an index. #96 via #95 (untergeek)
 * We won't talk about what happened to 1.1.1.  No really.  I hate git today :(

1.1.0 (12 June 2014)
--------------------

**General**

 * Updated! New command structure
 * New! Snapshot to fs or s3 #82 (untergeek)
 * New! Add/Remove indices to alias #82 via #86 (cschellenger)
 * New! ``--exclude-pattern`` #80 (ekamil)
 * New! (sort of) Restored ``--log-level`` support #73 (xavier-calland)
 * New! show command-line options #82 via #68 (untergeek)
 * New! Shard Allocation Routing #82 via #62 (nickethier)

**Bug fix**

 * Fix ``--max_num_segments`` not being passed correctly #74 (untergeek)
 * Change ``BUILD_NUMBER`` to ``CURATOR_BUILD_NUMBER`` in ``setup.py`` #60 (mohabusama)
 * Fix off-by-one error in time calculations #66 (untergeek)
 * Fix testing with python3 #92 (untergeek)

Errata

 * Removed ``optparse`` compatibility.  Now requires ``argparse``.

1.0.0 (25 Mar 2014)
-------------------

**General**

 * compatible with ``elasticsearch-py`` 1.0 and Elasticsearch 1.0 (honzakral)
 * Lots of tests! (honzakral)
 * Streamline code for 1.0 ES versions (honzakral)

**Bug fix**

 * Fix ``find_expired_indices()`` to not skip closed indices (honzakral)

0.6.2 (18 Feb 2014)
-------------------

**General**

 * Documentation fixes #38 (dharrigan)
 * Add support for HTTPS URI scheme and ``optparse`` compatibility for Python 2.6 (gelim)
 * Add elasticsearch module version checking for future compatibility checks (untergeek)

0.6.1 (08 Feb 2014)
-------------------

**General**

 * Added tarball versioning to ``setup.py`` (untergeek)

**Bug fix**

 * Fix ``long_description`` by including ``README.md`` in ``MANIFEST.in`` (untergeek)
 * Incorrect version number in ``curator.py`` (untergeek)

0.6.0 (08 Feb 2014)
-------------------

**General**

 * Restructured repository to a be a proper python package. (arieb)
 * Added ``setup.py`` file. (arieb)
 * Removed the deprecated file ``logstash_index_cleaner.py`` (arieb)
 * Updated ``README.md`` to fit the new package, most importantly the usage
   and installation. (arieb)
 * Fixes and package push to PyPI (untergeek)

0.5.2 (26 Jan 2014)
-------------------

**General**

 * Fix boolean logic determining hours or days for time selection (untergeek)

0.5.1 (20 Jan 2014)
-------------------

**General**

 * Fix ``can_bloom`` to compare numbers (HonzaKral)
 * Switched ``find_expired_indices()`` to use ``datetime`` and ``timedelta``
 * Do not try and catch unrecoverable exceptions. (HonzaKral)
 * Future proofing the use of the elasticsearch client (i.e. work with version
   1.0+ of Elasticsearch) (HonzaKral)
   Needs more testing, but should work.
 * Add tests for these scenarios (HonzaKral)

0.5.0 (17 Jan 2014)
-------------------

**General**

 * Deprecated ``logstash_index_cleaner.py``
   Use new ``curator.py`` instead (untergeek)
 * new script change: ``curator.py`` (untergeek)
 * new add index optimization (Lucene forceMerge) to reduce segments
   and therefore memory usage. (untergeek)
 * update refactor of args and several functions to streamline operation
   and make it more readable (untergeek)
 * update refactor further to clean up and allow immediate (and future)
   portability (HonzaKral)

0.4.0
-----

**General**

 * First version logged in ``CHANGELOG``
 * new ``--disable-bloom-days`` feature requires 0.90.9+

   http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index-modules-codec.html#bloom-postings

   This can save a lot of heap space on cold indexes (i.e. not actively indexing documents)
