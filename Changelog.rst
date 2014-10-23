.. _changelog:

Changelog
=========

2.1.0 (? ? ?)
-------------

**General**

 * Snapshot name now appears in log output or STDOUT. #178 (untergeek)
 * Replicas! You can now change the replica count of indices. Requested in #175 (untergeek)
 * Delay option added to Bloom Filter functionality. #206 (untergeek)

**Bug fixes**

 * Edge case where 1.4.0.Beta1-SNAPSHOT would break version check. Reported in #183 (untergeek)
 * Typo fixed. #193 (ferki)
 * Type fixed. #204 (gheppner)

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
