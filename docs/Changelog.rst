.. _changelog:

Changelog
=========

5.5.4 (23 May 2018)
-------------------

**Bug Fix**

  * Extra args in show.py prevented show_snapshots from executing (untergeek)


5.5.3 (21 May 2018)
-------------------

Short release cycle here specifically to address the Snapshot restore issue
raised in #1192

**Changes**

  * By default, filter out indices with ``index.lifecycle.name`` set.  This can
    be overridden with the option ``allow_ilm_indices`` with the caveat that
    you are on your own if there are conflicts. NOTE: The Index Lifecycle
    Management feature will not appear in Elasticsearch until 6.4.0
  * Removed some unused files from the repository.

**Bug Fixes**

  * Fix an ambiguously designed Alias test (untergeek)
  * Snapshot action will now raise an exception if the snapshot does not
    complete with state ``SUCCESS``. Reported in #1192 (untergeek)
  * The show_indices and show_snapshots singletons were not working within the
    new framework. They've been fixed now.

5.5.2 (14 May 2018)
-------------------

**Changes**

  * The ``alias``, ``restore``, ``rollover``, and ``shrink``  actions have been
    added to ``curator_cli``, along with a revamped method to manage/add
    actions in the future.
  * Updated ``certifi`` dependency to ``2018.4.16``
  * Added ``six`` dependency
  * Permit the use of versions 6.1 and greater of the ``elasticsearch`` python
    module.  There are issues with SSL contexts in the 6.0 release that prevent
    Curator from being able to use this version.  Currently the requirement
    version string is ``elasticsearch>=5.5.2,!=6.0.0,<7.0.0``
  * Start of pylint cleanup, and use of `six` `string_types`. (untergeek)

**Bug Fixes**

  * `unit_count_pattern` setting can cause indices to mistakenly be included
    in an index filter. Fixed in #1206 (soenkeliebau)
  * Fix rollover _check_max_size() call. Reported in #1202 by @diranged
    (untergeek).
  * Update tested versions of Elasticsearch. (untergeek).
  * Update setup.cfg to install dependencies during source install. (untergeek)
  * Fix reference to unset variable name in log output at
    https://github.com/elastic/curator/blob/v5.5.1/curator/actions.py#L2145
    It should be `idx` instead of `index`. (untergeek).
  * Alias action should raise `NoIndices` exception if `warn_if_no_indices` is
    `True`, and no `add` or `remove` sub-actions are found, rather than raising
    an `ActionError`. Reported in #1209 (untergeek).

**Documentation**

  * Clarify inclusive filtering for allocated filter. Fixed in #1203 (geekpete)
  * Fix Kibana filter description. #1199 (quartett-opa)
  * Add missing documentation about the ``new_name`` option for rollover.
    Reported in #1197 (untergeek)

5.5.1 (22 March 2018)
---------------------

**Bug Fixes**

  * Fix ``pip`` installation issues for older versions of Python
    #1183 (untergeek)

5.5.0 (21 March 2018)
---------------------

**New Features**

  * Add ``wait_for_rebalance`` as an option for ``shrink`` action. By default
    the behavior remains unchanged. You can now set this to False though to
    allow the shrink action to only check that the index being shrunk has
    finished being relocated and it will not wait for the cluster to
    rebalance. #1129 (tschroeder-zendesk)
  * Work around for extremely large cluster states. #1142 (rewiko)
  * Add CI tests for Elasticsearch versions 6.1 and 6.2 (untergeek)
  * Add Elasticsearch datemath support for snapshot names #1078 (untergeek)
  * Support ``max_size`` as a rollover condition for Elasticsearch versions
    6.1.0 and up. #1140 (untergeek)
  * Skip indices with a document count of 0 when using ``source: field_stats``
    to do ``age`` or ``period`` type filtering. #1130 (untergeek)

**Bug Fixes**

  * Fix missing node information in log line. #1142 (untergeek)
  * Fix default options in code that were causing schema validation errors
    after ``voluptuous`` upgrade to 0.11.1.
    Reported in #1149, fixed in #1156 (untergeek)
  * Disallow empty lists as reindex source.  Raise exception if that happens.
    Reported in #1139 (untergeek)
  * Set a ``timeout_override`` for ``delete_snapshots`` to catch cases where
    slower repository network and/or disk access can cause a snapshot delete
    to take longer than the default 30 second client timeout. #1133 (untergeek)
  * Add AWS ES 5.1 support. #1172 (wanix)
  * Add missing ``period`` filter arguments for ``delete_snapshots``.
    Reported in #1173 (untergeek)
  * Fix kibana filtertype to catch newer index names.
    Reported in #1171 (untergeek)
  * Re-order the closed indices filter for the Replicas action to take place
    `before` the empty list check.
    Reported in #1180 by ``@agomerz`` (untergeek)

**General**

  * Deprecate testing for Python 3.4.  It is no longer being supported by
    Python.
  * Increase logging to show error when ``master_only`` is true and there are
    multiple hosts.

**Documentation**

  * Correct a misunderstanding about the nature of rollover conditions.
    #1144 (untergeek)
  * Correct links to the field_stats API, as it is non-existent in
    Elasticsearch 6.x. (untergeek)
  * Add a warning about using forcemerge on active indices.
    #1153 (untergeek)
  * Fix select URLs in pip installation from source to not be 404
    #1133 (untergeek)
  * Fix an error in regex filter documentation #1138 (arne-cl)

5.4.1 (6 December 2017)
-----------------------

**Bug Fixes**

  * Improve Dockerfile to build from source and produce slimmer image
    #1111 (mikn)
  * Fix ``filter_kibana`` to correctly use ``exclude`` argument
    #1116 (cjuroz)
  * Fix `ssl_no_validate` behavior within AWS ES #1118 (igalarzab)
  * Improve command-line exception management #1119 (4383)
  * Make ``alias`` action always process ``remove`` before ``add``
    to prevent undesired alias removals. #1120 (untergeek)

**General**

  * Bump ES versions in Travis CI

**Documentation**

  * Remove ``unit_count`` parameter doc for parameter that no longer
    exists #1107 (dashford)
  * Add missing ``exclude: True`` in ``timestring`` docs #1117 (GregMefford)



5.4.0 (13 November 2017)
------------------------

**Announcement**

  * Support for Elasticsearch 6.0!!! Yes!

**New Features**

  * The ``field_stats`` API may be gone from Elasticsearch, but its utility
    cannot be denied.  And so, Curator has replaced the ``field_stats`` API
    call with a small aggregation query.  This will be perhaps a bit more
    costly in performance terms, as this small aggregation query must be made
    to each index in sequence, rather than as a one-shot call, like the
    ``field_stats`` API call.  But the benefit will remain available, and
    it's the only major API that did not persevere between Elasticsearch 5.x
    and 6.x that was needed by Curator.

5.3.0 (31 October 2017)
-----------------------

**New Features**

  * With the period filter and field_stats, it is useful to match indices
    that fit `within` the period, rather than just their start dates.  This
    is now possible with ``intersect``.  See more in the documentation.
    Requested in #1045. (untergeek)
  * Add a ``restore`` function to ``curator_cli`` singleton. Mentioned in
    #851 (alexef)
  * Add ``pattern`` to the ``count`` filter.  This is particularly useful
    when working with rollover indices.  Requested in #1044 (untergeek)
  * The ``es_repo_mgr create`` command now can take ``skip_repo_fs_check`` as
    an argument (default is False) #1072 (alexef)
  * Add ``pattern_type`` feature expansion to the ``period`` filter.  The
    default behavior is ``pattern_type='relative'``, which preserves existing
    behaviors so users with existing configurations can continue to use them
    without interruption.  The new ``pattern_type`` is ``absolute``, which
    allows you to specify hard dates for ``date_from`` and ``date_to``, while
    ``date_from_format`` and ``date_to_format`` are strftime strings to
    interpret the from and to dates. Requested in #1047 (untergeek)
  * Add ``copy_aliases`` option to the ``shrink`` action. So this option is
    only set in the ``shrink`` action. The default value of the option is
    ``copy_aliases: 'False'`` and it does nothing. If you set to
    ``copy_aliases: 'True'``, you could copy the aliases from the source index
    to the target index. Requested in #1060 (monkey3199)
  * IAM Credentials can now be retrieved from the environment using the Boto3
    Credentials provider. #1084 (kobuskc)

**Bug Fixes**

  * Delete the target index (if it exists) in the event that a shrink fails.
    Requested in #1058 (untergeek)
  * Fixed an integration test that could fail in the waning days of a month.
  * Fix build system anomalies for both unix and windows.

**Documentation**

  * Set repository access to be https by default.
  * Add documentation for ``copy_aliases`` option.

5.2.0 (1 September 2017)
------------------------

**New Features**

  * Shrink action! Apologies to all who have patiently waited for this
    feature.  It's been a long time coming, but it is hopefully worth the
    wait.  There are a lot of checks and tests associated with this action,
    as there are many conditions that have to be met in order for a shrink
    to take place.  Curator will try its best to ensure that all of these
    conditions are met so you can comfortably rest assured that shrink will
    work properly unattended.  See the documentation for more information.
  * The ``cli`` function has been split into ``cli`` and ``run`` functions.
    The behavior of ``cli`` will be indistinguishable from previous releases,
    preserving API integrity.  The new ``run`` function allows lambda and other
    users to `run` Curator from the API with only a client configuration file
    and action file as arguments.  Requested in #1031 (untergeek)
  * Allow use of time/date string interpolation for Rollover index naming.
    Added in #1010 (tschroeder-zendesk)
  * New ``unit_count_pattern`` allows you to derive the ``unit_count`` from
    the index name itself.  This involves regular expressions, so be sure to
    do lots of testing in ``--dry-run`` mode before deploying to production.
    Added by (soenkeliebau) in #997

**Bug Fixes**

  * Reindex ``request_body`` allows for 2 different ``size`` options.  One
    limits the number of documents reindexed.  The other is for batch sizing.
    The batch sizing option was missing from the schema validator.  This has
    been corrected.  Reported in #1038 (untergeek)
  * A few sundry logging and notification changes were made.

5.1.2 (08 August 2017)
----------------------

**Errata**

  * An update to Elasticsearch 5.5.0 changes the behavior of
    ``filter_by_aliases``, differing from previous 5.x versions.

    If a list of aliases is provided, indices must appear in `all` listed
    aliases or a 404 error will result, leading to no indices being matched.
    In older versions, if the index was associated with even one of the
    aliases in aliases, it would result in a match.

    Tests and documentation have been updated to address these changes.

  * Debian 9 changed SSL versions, which means that the pre-built debian
    packages no longer work in Debian 9.  In the short term, this requires
    a new repository.  In the long term, I will try to get a better
    repository system working for these so they all work together, better.
    Requested in #998 (untergeek)

**Bug Fixes**

  * Support date math in reindex operations better.  It did work previously,
    but would report failure because the test was looking for the index with
    that name from a list of indices, rather than letting Elasticsearch do
    the date math.  Reported by DPattee in #1008 (untergeek)
  * Under rare circumstances, snapshot delete (or create) actions could fail,
    even when there were no snapshots in state ``IN_PROGRESS``.  This was
    tracked down by JD557 as a collision with a previously deleted snapshot
    that hadn't finished deleting.  It could be seen in the tasks API.  An
    additional test for snapshot activity in the tasks API has been added to
    cover this scenario.  Reported in #999 (untergeek)
  * The ``restore_check`` function did not work properly with wildcard index
    patterns.  This has been rectified, and an integration test added to
    satisfy this.  Reported in #989 (untergeek)
  * Make Curator report the Curator version, and not just reiterate the
    elasticsearch version when reporting version incompatibilities. Reported
    in #992. (untergeek)
  * Fix repository/snapshot name logging issue. #1005 (jpcarey)
  * Fix Windows build issue #1014 (untergeek)


**Documentation**

  * Fix/improve rST API documentation.
  * Thanks to many users who not only found and reported documentation issues,
    but also submitted corrections.

5.1.1 (8 June 2017)
-------------------

**Bug Fixes**

  * Mock and cx_Freeze don't play well together.  Packages weren't working, so
    I reverted the string-based comparison as before.

5.1.0 (8 June 2017)
-------------------

**New Features**

  * Index Settings are here! First requested as far back as #160, it's been
    requested in various forms culminating in #656.  The official documentation
    addresses the usage. (untergeek)
  * Remote reindex now adds the ability to migrate from one cluster to another,
    preserving the index names, or optionally adding a prefix and/or a suffix.
    The official documentation shows you how. (untergeek)
  * Added support for naming rollover indices. #970 (jurajseffer)
  * Testing against ES 5.4.1, 5.3.3

**Bug Fixes**

  * Since Curator no longer supports old versions of python, convert tests to
    use ``isinstance``. #973 (untergeek)
  * Fix stray instance of ``is not`` comparison instead of ``!=`` #972
    (untergeek)
  * Increase remote client timeout to 180 seconds for remote reindex. #930
    (untergeek)

**General**

  * elasticsearch-py dependency bumped to 5.4.0
  * Added mock dependency due to isinstance and testing requirements
  * AWS ES 5.3 officially supports Curator now.  Documentation has been updated
    to reflect this.

5.0.4 (16 May 2017)
-------------------

**Bug Fixes**

  * The ``_recovery`` check needs to compare using ``!=`` instead of
    ``is not``, which apparently does not accurately compare unicode strings.
    Reported in #966 (untergeek)

5.0.3 (15 May 2017)
-------------------

**Bug Fixes**

  * Restoring a snapshot on an exceptionally fast cluster/node can create a
    race condition where a ``_recovery`` check returns an empty dictionary
    ``{}``, which causes Curator to fail.  Added test and code to correct this.
    Reported in #962. (untergeek)

5.0.2 (4 May 2017)
------------------

**Bug Fixes**

  * Nasty bug in schema validation fixed where boolean options or filter flags
    would validate as ``True`` if non-boolean types were submitted.
    Reported in #945. (untergeek)
  * Check for presence of alias after reindex, in case the reindex was to an
    alias. Reported in #941. (untergeek)
  * Fix an edge case where an index named with `1970.01.01` could not be sorted
    by index-name age. Reported in #951. (untergeek)
  * Update tests to include ES 5.3.2
  * Bump certifi requirement to 2017.4.17.

**Documentation**

  * Document substitute strftime symbols for doing ISO Week timestrings added
    in #932. (untergeek)
  * Document how to include file paths better. Fixes #944. (untergeek)

5.0.1 (10 April 2017)
---------------------

**Bug Fixes**

  * Fixed default values for ``include_global_state`` on the restore
    action to be in line with defaults in Elasticsearch 5.3

**Documentation**

  * Huge improvement to documenation, with many more examples.
  * Address age filter limitations per #859 (untergeek)
  * Address date matching behavior better per #858 (untergeek)

5.0.0 (5 April 2017)
--------------------

The full feature set of 5.0 (including alpha releases) is included here.

**New Features**

  * Reindex is here! The new reindex action has a ton of flexibility. You
    can even reindex from remote locations, so long as the remote cluster is
    Elasticsearch 1.4 or newer.
  * Added the ``period`` filter (#733). This allows you to select indices
    or snapshots, based on whether they fit within a period of hours, days,
    weeks, months, or years.
  * Add dedicated "wait for completion" functionality. This supports health
    checks, recovery (restore) checks, snapshot checks, and operations which
    support the new tasks API.  All actions which can use this have been
    refactored to take advantage of this.  The benefit of this new feature is
    that client timeouts will be less likely to happen when performing long
    operations, like snapshot and restore.

    NOTE: There is one caveat: forceMerge does not support this, per the
    Elasticsearch API. A forceMerge call will hold the client until complete,
    or the client times out.  There is no clean way around this that I can
    discern.
  * Elasticsearch date math naming is supported and documented for the
    ``create_index`` action.  An integration test is included for validation.
  * Allow allocation action to unset a key/value pair by using an empty value.
    Requested in #906. (untergeek)
  * Added support for the Rollover API. Requested in #898, and by countless
    others.
  * Added ``warn_if_no_indices`` option for ``alias`` action in response to
    #883.  Using this option will permit the ``alias`` add or remove to
    continue with a logged warning, even if the filters result in a
    ``NoIndices`` condition. Use with care.

**General**

  * Bumped ``click`` (python module) version dependency to 6.7
  * Bumped ``urllib3`` (python module) version dependency to 1.20
  * Bumped ``elasticsearch`` (python module) version dependency to 5.3
  * Refactored a ton of code to be cleaner and hopefully more consistent.

**Bug Fixes**

  * Curator now logs version incompatibilities as an error, rather than just
    raising an Exception. #874 (untergeek)
  * The ``get_repository()`` function now properly raises an exception instead
    of returning `False` if nothing is found. #761 (untergeek)
  * Check if an index is in an alias before attempting to delete it from the
    alias.  Issue raised in #887. (untergeek)
  * Fix allocation issues when using Elasticsearch 5.1+. Issue raised in #871
    (untergeek)

**Documentation**

  * Add missing repository arg to auto-gen API docs. Reported in #888
    (untergeek)
  * Add all new documentation and clean up for v5 specific.

**Breaking Changes**

  * IndexList no longer checks to see if there are indices on initialization.


5.0.0a1 (23 March 2017)
-----------------------

This is the first alpha release of Curator 5.  This should not be used for
production! There `will` be many more changes before 5.0.0 is released.

**New Features**

  * Allow allocation action to unset a key/value pair by using an empty value.
    Requested in #906. (untergeek)
  * Added support for the Rollover API. Requested in #898, and by countless
    others.
  * Added ``warn_if_no_indices`` option for ``alias`` action in response to
    #883.  Using this option will permit the ``alias`` add or remove to
    continue with a logged warning, even if the filters result in a
    ``NoIndices`` condition. Use with care.

**Bug Fixes**

  * Check if an index is in an alias before attempting to delete it from the
    alias.  Issue raised in #887. (untergeek)
  * Fix allocation issues when using Elasticsearch 5.1+. Issue raised in #871
    (untergeek)

**Documentation**

  * Add missing repository arg to auto-gen API docs. Reported in #888
    (untergeek)

4.2.6 (27 January 2016)
-----------------------

**General**

  * Update Curator to use version 5.1 of the ``elasticsearch-py`` python
    module. With this change, there will be no reverse compatibility with
    Elasticsearch 2.x.  For 2.x versions, continue to use the 4.x branches of
    Curator.
  * Tests were updated to reflect the changes in API calls, which were minimal.
  * Remove "official" support for Python 2.6. If you must use Curator on a
    system that uses Python 2.6 (RHEL/CentOS 6 users), it is recommended that
    you use the official RPM package as it is a frozen binary built on Python
    3.5.x which will not conflict with your system Python.
  * Use ``isinstance()`` to verify client object. #862 (cp2587)
  * Prune older versions from Travis CI tests.
  * Update ``certifi`` dependency to latest version

**Documentation**

  * Add version compatibility section to official documentation.
  * Update docs to reflect changes.  Remove cruft and references to older
    versions.

4.2.5 (22 December 2016)
------------------------

**General**

  * Add and increment test versions for Travis CI. #839 (untergeek)
  * Make `filter_list` optional in snapshot, show_snapshot and show_indices
    singleton actions. #853 (alexef)

**Bug Fixes**

  * Fix cli integration test when different host/port are specified.  Reported
    in #843 (untergeek)
  * Catch empty list condition during filter iteration in singleton actions.
    Reported in #848 (untergeek)

**Documentation**

  * Add docs regarding how filters are ANDed together, and how to do an OR with
    the regex pattern filter type. Requested in #842 (untergeek)
  * Fix typo in Click version in docs. #850 (breml)
  * Where applicable, replace `[source,text]` with `[source,yaml]` for better
    formatting in the resulting docs.

4.2.4 (7 December 2016)
-----------------------

**Bug Fixes**

  * ``--wait_for_completion`` should be `True` by default for Snapshot
    singleton action.  Reported in #829 (untergeek)
  * Increase `version_max` to 5.1.99. Prematurely reported in #832 (untergeek)
  * Make the '.security' index visible for snapshots so long as proper
    credentials are used. Reported in #826 (untergeek)

4.2.3.post1 (22 November 2016)
------------------------------

This fix is `only` going in for ``pip``-based installs.  There are no other
code changes.

**Bug Fixes**

  * Fixed incorrect assumption of PyPI picking up dependency for certifi.  It
    is still a dependency, but should not affect ``pip`` installs with an error
    any more.  Reported in #821 (untergeek)


4.2.3 (21 November 2016)
------------------------

4.2.2 was pulled immediately after release after it was discovered that the
Windows binary distributions were still not including the certifi-provided
certificates.  This has now been remedied.

**General**

  * ``certifi`` is now officially a requirement.
  * ``setup.py`` now forcibly includes the ``certifi`` certificate PEM file in
    the "frozen" distributions (i.e., the compiled versions).  The
    ``get_client`` method was updated to reflect this and catch it for both the
    Linux and Windows binary distributions.  This should `finally` put to rest
    #810

4.2.2 (21 November 2016)
------------------------

**Bug Fixes**

  * The certifi-provided certificates were not propagating to the compiled
    RPM/DEB packages.  This has been corrected.  Reported in #810 (untergeek)

**General**

  * Added missing ``--ignore_empty_list`` option to singleton actions.
    Requested in #812 (untergeek)

**Documentation**

  * Add a FAQ entry regarding the click module's need for Unicode when using
    Python 3.  Kind of a bug fix too, as the entry_points were altered to catch
    this omission and report a potential solution on the command-line. Reported
    in #814 (untergeek)
  * Change the "Command-Line" documentation header to be "Running Curator"

4.2.1 (8 November 2016)
-----------------------

**Bug Fixes**

  * In the course of package release testing, an undesirable scenario was
    caught where boolean flags default values for ``curator_cli`` were
    improperly overriding values from a yaml config file.

**General**

  * Adding in direct download URLs for the RPM, DEB, tarball and zip packages.

4.2.0 (4 November 2016)
-----------------------

**New Features**

  * Shard routing allocation enable/disable. This will allow you to disable
    shard allocation routing before performing one or more actions, and then
    re-enable after it is complete. Requested in #446 (untergeek)
  * Curator 3.x-style command-line.  This is now ``curator_cli``, to
    differentiate between the current binary.  Not all actions are available,
    but the most commonly used ones are.  With the addition in 4.1.0 of schema
    and configuration validation, there's even a way to still do filter
    chaining on the command-line! Requested in #767, and by many other
    users (untergeek)

**General**

  * Update testing to the most recent versions.
  * Lock elasticsearch-py module version at >= 2.4.0 and <= 3.0.0.  There are
    API changes in the 5.0 release that cause tests to fail.

**Bug Fixes**

  * Guarantee that binary packages are built from the latest Python +
    libraries. This ensures that SSL/TLS will work without warning messages
    about insecure connections, unless they actually are insecure. Reported in
    #780, though the reported problem isn't what was fixed. The fix is needed
    based on what was discovered while troubleshooting the problem. (untergeek)

4.1.2 (6 October 2016)
----------------------

This release does not actually add any new code to Curator, but instead
improves documentation and includes new linux binary packages.

**General**

  * New Curator binary packages for common Linux systems!
    These will be found in the same repositories that the python-based packages
    are in, but have no dependencies.  All necessary libraries/modules are
    bundled with the binary, so everything should work out of the box.
    This feature doesn't change any other behavior, so it's not a major
    release.

    These binaries have been tested in:
      * CentOS 6 & 7
      * Ubuntu 12.04, 14.04, 16.04
      * Debian 8

    They do not work in Debian 7 (library mismatch).  They may work in other
    systems, but that is untested.

    The script used is in the unix_packages directory.  The Vagrantfiles for
    the various build systems are in the Vagrant directory.

**Bug Fixes**

  * The only bug that can be called a bug is actually a stray ``.exe`` suffix
    in the binary package creation section (cx_freeze) of ``setup.py``.  The
    Windows binaries should have ``.exe`` extensions, but not unix variants.
  * Elasticsearch 5.0.0-beta1 testing revealed that a document ID is required
    during document creation in tests.  This has been fixed, and a redundant
    bit of code in the forcemerge integration test was removed.

**Documentation**

  * The documentation has been updated and improved.  Examples and installation
    are now top-level events, with the sub-sections each having their own link.
    They also now show how to install and use the binary packages, and the
    section on installation from source has been improved.  The missing
    section on installing the voluptuous schema verification module has been
    written and included. #776 (untergeek)

4.1.1 (27 September 2016)
-------------------------

**Bug Fixes**

  * String-based booleans are now properly coerced.  This fixes an issue where
    `True`/`False` were used in environment variables, but not recognized.
    #765 (untergeek)

  * Fix missing `count` method in ``__map_method`` in SnapshotList. Reported in
    #766 (untergeek)

**General**

  * Update es_repo_mgr to use the same client/logging YAML config file.
    Requested in #752 (untergeek)

**Schema Validation**

  * Cases where ``source`` was not defined in a filter (but should have been)
    were informing users that a `timestring` field was there that shouldn't
    have been.  This edge case has been corrected.

**Documentation**

  * Added notifications and FAQ entry to explain that AWS ES is not supported.

4.1.0 (6 September 2016)
------------------------

**New Features**

  * Configuration and Action file schema validation.  Requested in #674
    (untergeek)
  * Alias filtertype! With this filter, you can select indices based on whether
    they are part of an alias.  Merged in #748 (untergeek)
  * Count filtertype! With this filter, you can now configure Curator to only
    keep the most recent `n` indices (or snapshots!).  Merged in #749
    (untergeek)
  * Experimental! Use environment variables in your YAML configuration files.
    This was a popular request, #697. (untergeek)

**General**

  * New requirement! ``voluptuous`` Python schema validation module
  * Requirement version bump:  Now requires ``elasticsearch-py`` 2.4.0

**Bug Fixes**

  * ``delete_aliases`` option in ``close`` action no longer results in an error
    if not all selected indices have an alias.  Add test to confirm expected
    behavior. Reported in #736 (untergeek)

**Documentation**

  * Add information to FAQ regarding indices created before Elasticsearch 1.4.
    Merged in #747

4.0.6 (15 August 2016)
----------------------

**Bug Fixes**

  * Update old calls used with ES 1.x to reflect changes in 2.x+. This was
    necessary to work with Elasticsearch 5.0.0-alpha5.
    Fixed in #728 (untergeek)

**Doc Fixes**

  * Add section detailing that the value of a ``value`` filter element should
    be encapsulated in single quotes. Reported in #726. (untergeek)

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
    `creation_date` field in settings.  This field was introduced in ES v1.4,
    so that indicates a rather old index. #663 (untergeek)
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
  * State of indices pulled and stored in `IndexList` instance.  Fewer API
    calls required to serially test for open/close, `size_in_bytes`, etc.
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
  * Class: `SnapshotList`. This pulls all snapshots from the given repository
    at instantiation, and you apply filters, which are class methods.  You can
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
  * State of indices pulled and stored in `IndexList` instance.  Fewer API
    calls required to serially test for open/close, `size_in_bytes`, etc.
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
  * Class: `SnapshotList`. This pulls all snapshots from the given repository
    at instantiation, and you apply filters, which are class methods.  You can
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
  * Optionally delete aliases from indices before closing.
    Fixes #644 (untergeek)

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

  * Alias action can now accept `extra_settings` to allow adding filters,
    and/or routing.


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
  * Class: `SnapshotList`. This pulls all snapshots from the given repository
    at instantiation, and you apply filters, which are class methods.  You can
    iterate over as many filters as you like, in fact, due to the YAML config
    file.
  * YAML configuration files.  Now a single file can define an entire batch of
    commands, each with their own filters, to be performed in sequence.
  * Atomically add/remove indices from aliases! This is possible by way of the
    new `IndexList` class and YAML configuration files.
  * State of indices pulled and stored in `IndexList` instance.  Fewer API
    calls required to serially test for open/close, `size_in_bytes`, etc.
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

  * Add support for the `--client-cert` and `--client-key` command line
    parameters and client_cert and client_key parameters to the get_client()
    call. #520 (richm)

**Bug fixes**

  * Disallow users from creating snapshots with upper-case letters, which is
    not permitted by Elasticsearch. #562 (untergeek)
  * Remove `print()` command from ``setup.py`` as it causes issues with
    command-line retrieval of ``--url``, etc. #568 (thib-ack)
  * Remove unnecessary argument from `build_filter()` #530 (zzugg)
  * Allow day of year filter to be made up with 1, 2 or 3 digits
    #578 (petitout)


3.4.1 (10 February 2016)
------------------------

**General**

  * Update license copyright to 2016
  * Use slim python version with Docker #527 (xaka)
  * Changed ``--master-only`` exit code to 0 when connected to non-master node
    #540 (wkruse)
  * Add ``cx_Freeze`` capability to ``setup.py``, plus a ``binary_release.py``
    script to simplify binary package creation.  #554 (untergeek)
  * Set Elastic as author. #555 (untergeek)
  * Put repository creation methods into API and document them. Requested in
    #550 (untergeek)

**Bug fixes**

  * Fix sphinx documentation build error #506 (hydrapolic)
  * Ensure snapshots are found before iterating #507 (garyelephant)
  * Fix a doc inconsistency #509 (pmoust)
  * Fix a typo in `show` documentation #513 (pbamba)
  * Default to trying the cluster state for checking whether indices are
    closed, and then fall back to using the _cat API (for Amazon ES instances).
    #519 (untergeek)
  * Improve logging to show time delay between optimize runs, if selected.
    #525 (untergeek)
  * Allow elasticsearch-py module versions through 2.3.0 (a presumption at this
    point) #524 (untergeek)
  * Improve logging in snapshot api method to reveal when a repository appears
    to be missing. Reported in #551 (untergeek)
  * Test that ``--timestring`` has the correct variable for ``--time-unit``.
    Reported in #544 (untergeek)
  * Allocation will exit with exit_code 0 now when there are no indices to work
    on. Reported in #531 (untergeek)


3.4.0 (28 October 2015)
-----------------------

**General**

  * API change in elasticsearch-py 1.7.0 prevented alias operations.  Fixed in
    #486 (HonzaKral)
  * During index selection you can now select only closed indices with
    ``--closed-only``. Does not impact ``--all-indices`` Reported in #476.
    Fixed in #487 (Basster)
  * API Changes in Elasticsearch 2.0.0 required some refactoring.  All tests
    pass for ES versions 1.0.3 through 2.0.0-rc1.  Fixed in #488 (untergeek)
  * es_repo_mgr now has access to the same SSL options from #462.
    #489 (untergeek)
  * Logging improvements requested in #475. (untergeek)
  * Added ``--quiet`` flag. #494 (untergeek)
  * Fixed ``index_closed`` to work with AWS Elasticsearch. #499 (univerio)
  * Acceptable versions of Elasticsearch-py module are 1.8.0 up to
    2.1.0 (untergeek)

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

  * Allocation type can now also be ``include`` or ``exclude``, in addition to
    the existing default ``require`` type. Add ``--type`` to the allocation
    command to specify the type. #443 (steffo)

  * Bump elasticsearch python module dependency to 1.6.0+ to enable
    synced_flush API call. Reported in #447 (untergeek)

  * Add SSL features, ``--ssl-no-validate`` and ``certificate`` to provide
    other ways to validate SSL connections to Elasticsearch. #436 (untergeek)

**Bug fixes**

  * Delete by space was only reporting space used by primary shards.  Fixed to
    show all space consumed.  Reported in #455 (untergeek)

  * Update exit codes and messages for snapshot selection.  Reported in
    #452 (untergeek)

  * Fix potential int/float casting issues. Reported in #465 (untergeek)

3.2.3 (16 July 2015)
--------------------

**Bug fix**

  * In order to address customer and community issues with bulk deletes, the
    ``master_timeout`` is now invoked for delete operations.  This should
    address 503s with 30s timeouts in the debug log, even when ``--timeout`` is
    set to a much higher value.  The ``master_timeout`` is tied to the
    ``--timeout`` flag value, but will not exceed 300 seconds. #420 (untergeek)

**General**

  * Mixing it up a bit here by putting `General` second!  The only other
    changes are that logging has been improved for deletes so you won't need to
    have the ``--debug`` flag to see if you have error codes >= 400, and some
    code documentation improvements.

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

  * Added delete verification & retry (fixed at 3x) to potentially cover an
    edge case in #420 (untergeek)
  * Since GitHub allows rST (reStructuredText) README documents, and that's
    what PyPI wants also, the README has been rebuilt in rST. (untergeek)

**Bug fixes**

  * If closing indices with ES 1.6+, and all indices are closed, ensure that
    the seal command does not try to seal all indices.  Reported in
    #426 (untergeek)
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
   ``org.elasticsearch.ElasticsearchIllegalArgumentException: Can't update
   [index.number_of_replicas] on closed indices [[test_index]] - can leave
   index in an unopenable state``
   As a result, the ``change_replicas`` method has been updated to prune closed
   indices.  This change will apply to all versions of Elasticsearch.
   Reported in #400 (untergeek)
 * Fixed es_repo_mgr repository creation verification error. Reported in #389
   (untergeek)



3.1.0 (21 May 2015)
-------------------

**General**

 * If ``wait_for_completion`` is true, snapshot success is now tested and
   logged. Reported in #253 (untergeek)
 * Log & return false if a snapshot is already in progress (untergeek)
 * Logs individual deletes per index, even though they happen in batch mode.
   Also log individual snapshot deletions. Reported in #372 (untergeek)
 * Moved ``chunk_index_list`` from cli to api utils as it's now also used by
   ``filter.py``
 * Added a warning and 10 second timer countdown if you use ``--timestring``
   to filter indices, but do not use ``--older-than`` or ``--newer-than`` in
   conjunction with it. This is to address #348, which behavior isn't a bug,
   but prevents accidental action against all of your time-series indices. The
   warning and timer are not displayed for ``show`` and ``--dry-run``
   operations.
 * Added tests for ``es_repo_mgr`` in #350
 * Doc fixes

**Bug fixes**

 * delete-by-space needed the same fix used for #245. Fixed in #353 (untergeek)
 * Increase default client timeout for ``es_repo_mgr`` as node discovery and
   availability checks for S3 repositories can take a bit.  Fixed in
   #352 (untergeek)
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

 * Purge unneeded constants, and clean up config options for snapshot. Reported
   in #303 (untergeek)
 * Don't split large index list if performing snapshots. Reported in
   #307 (untergeek)
 * Act correctly if a zero value for `--older-than` or `--newer-than` is
   provided. #309 (untergeek)

3.0.1 (16 Mar 2015)
-------------------

**Announcement**

The ``regex_iterate`` method was horribly named.  It has been renamed to
``apply_filter``.  Methods have been added to allow API users to build a
filtered list of indices similarly to how the CLI does.  This was an oversight.
Props to @SegFaultAX for pointing this out.

**General**

 * In conjunction with the rebrand to Elastic, URLs and documentation were
   updated.
 * Renamed horribly named `regex_iterate` method to `apply_filter`
   #298 (untergeek)
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

Because 3.0 is a major point release, there have been some major changes to
both the API as well as the CLI arguments and structure.

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
   with curator.  All tests now pass for both Python2 and Python3, with 99%
   code coverage in both environments.
 * **New CLI library.** Using Click now. http://click.pocoo.org/3/
   This change is especially important as it allows very easy CLI integration
   testing.
 * **Pipelined filtering!** You can now use ``--older-than`` & ``--newer-than``
   in the same command!  You can also provide your own regex via the
   ``--regex`` parameter.  You can use multiple instances of the ``--exclude``
   flag.
 * **Manually include indices!** With the ``--index`` paramter, you can add an
   index to the working list.  You can provide multiple instances of the
   ``--index`` parameter as well!
 * **Tests!** So many tests now.  Test coverage of the API methods is at 100%
   now, and at 99% for the CLI methods.  This doesn't mean that all of the
   tests are perfect, or that I haven't missed some scenarios.  It does mean,
   however, that it will be much easier to write tests if something turns up
   missed.  It also means that any new functionality will now need to have
   tests.
 * **Iteration changes** Methods now only iterate through each index when
   appropriate!  In fact, the only commands that iterate are `alias` and
   `optimize`.  The `bloom` command will iterate, but only if you have added
   the `--delay` flag with a value greater than zero.
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
   with curator.  All tests now pass for both Python2 and Python3, with 99%
   code coverage in both environments.
 * **New CLI library.** Using Click now. http://click.pocoo.org/3/
   This change is especially important as it allows very easy CLI integration
   testing.
 * **Pipelined filtering!** You can now use ``--older-than`` & ``--newer-than``
   in the same command!  You can also provide your own regex via the
   ``--regex`` parameter.  You can use multiple instances of the ``--exclude``
   flag.
 * **Manually include indices!** With the ``--index`` paramter, you can add an
   index to the working list.  You can provide multiple instances of the
   ``--index`` parameter as well!
 * **Tests!** So many tests now.  Test coverage of the API methods is at 100%
   now, and at 99% for the CLI methods.  This doesn't mean that all of the
   tests are perfect, or that I haven't missed some scenarios.  It does mean,
   however, that it will be much easier to write tests if something turns up
   missed.  It also means that any new functionality will now need to have
   tests.
 * Methods now only iterate through each index when appropriate!
 * Improved packaging!  Hopefully the ``entry_point`` issues some users have
   had will be addressed by this.  Methods have been moved into categories of
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
   in the same command!  You can also provide your own regex via the
   ``--regex`` parameter.  You can use multiple instances of the ``--exclude``
   flag.
 * **Manually include indices!** With the ``--index`` paramter, you can add an
   index to the working list.  You can provide multiple instances of the
   ``--index`` parameter as well!
 * **Tests!** So many tests now.  Unit test coverage of the API methods is at
   100% now.  This doesn't mean that all of the tests are perfect, or that I
   haven't missed some scenarios.  It does mean that any new functionality will
   need to also have tests, now.
 * Methods now only iterate through each index when appropriate!
 * Improved packaging!  Hopefully the ``entry_point`` issues some users have
   had will be addressed by this.  Methods have been moved into categories of
   ``api`` and ``cli``, and further broken out into individual modules to help
   them be easier to find and read.
 * Check for allocation before potentially re-applying an allocation rule.
   #273 (ferki)

**Bug fixes**

 * Don't accidentally delete ``.kibana`` index. #261 (malagoli)
 * Fix segment count for empty indices. #265 (untergeek)
 * Change bloom filter cutoff Elasticsearch version to 1.4. Reported in
   #267 (untergeek)


2.1.2 (22 January 2015)
-----------------------

**Bug fixes**

 * Do not try to set replica count if count matches provided argument.
   #247 (bobrik)
 * Fix JSON logging (Logstash format). #250 (magnusbaeck)
 * Fix bug in `filter_by_space()` which would match all indices if the provided
   patterns found no matches. Reported in #254 (untergeek)

2.1.1 (30 December 2014)
------------------------

**Bug fixes**

 * Renamed unnecessarily redundant ``--replicas`` to ``--count`` in args for
   ``curator_script.py``

2.1.0 (30 December 2014)
------------------------

**General**

 * Snapshot name now appears in log output or STDOUT. #178 (untergeek)
 * Replicas! You can now change the replica count of indices. Requested in #175
   (untergeek)
 * Delay option added to Bloom Filter functionality. #206 (untergeek)
 * Add 2-digit years as acceptable pattern (y vs. Y). Reported in #209
   (untergeek)
 * Add Docker container definition #226 (christianvozar)
 * Allow the use of 0 with --older-than, --most-recent and
   --delete-older-than. See #208. #211 (bobrik)

**Bug fixes**

 * Edge case where 1.4.0.Beta1-SNAPSHOT would break version check. Reported in
   #183 (untergeek)
 * Typo fixed. #193 (ferki)
 * Type fixed. #204 (gheppner)
 * Shows proper error in the event of concurrent snapshots. #177 (untergeek)
 * Fixes erroneous index display of ``_, a, l, l`` when --all-indices selected.
   Reported in #222 (untergeek)
 * Use json.dumps() to escape exceptions. Reported in #210 (untergeek)
 * Check if index is closed before adding to alias.  Reported in #214 (bt5e)
 * No longer force-install argparse if pre-installed #216 (whyscream)
 * Bloom filters have been removed from Elasticsearch 1.5.0. Update methods
   and tests to act accordingly. #233 (untergeek)

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

 * New! Separation of Elasticsearch Curator Python API and
   curator_script.py (untergeek)
 * New! ``--delay`` after optimize to allow cluster to quiesce #131 (untergeek)
 * New! ``--suffix`` option in addition to ``--prefix`` #136 (untergeek)
 * New! Support for wildcards in prefix & suffix #136 (untergeek)
 * Complete refactor of snapshots.  Now supporting incrementals! (untergeek)

**Bug fix**

 * Incorrect error msg if no indices sent to create_snapshot (untergeek)
 * Correct for API change coming in ES 1.4 #168 (untergeek)
 * Missing ``"`` in Logstash log format #143 (cassianoleal)
 * Change non-master node test to exit code 0, log as ``INFO``.
   #145 (untergeek)
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
 * Fixed ``show`` command bug caused by changes to command structure
   #126 (michaelweiser)

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

 * The ``--separator`` option was removed in lieu of user-specified date
   patterns.
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

 * This was a showstopper bug for anyone using RHEL/CentOS with a
   Python 2.6 dependency for yum
 * Python 2.6 does not like format calls without an index. #96 via #95
   (untergeek)
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
 * Change ``BUILD_NUMBER`` to ``CURATOR_BUILD_NUMBER`` in ``setup.py``
   #60 (mohabusama)
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
 * Add support for HTTPS URI scheme and ``optparse`` compatibility for Python
   2.6 (gelim)
 * Add elasticsearch module version checking for future compatibility checks
   (untergeek)

0.6.1 (08 Feb 2014)
-------------------

**General**

 * Added tarball versioning to ``setup.py`` (untergeek)

**Bug fix**

 * Fix ``long_description`` by including ``README.md`` in ``MANIFEST.in``
   (untergeek)
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

   This can save a lot of heap space on cold indexes (i.e. not actively
   indexing documents)
