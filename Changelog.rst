.. _changelog:

Changelog
=========

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

The API docs are still at http://curator.readthedocs.org.  Be sure to read the
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
