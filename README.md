# Curator [![Build Status](http://build-eu-00.elastic.co/job/es-curator_core/badge/icon)](http://build-eu-00.elastic.co/job/es-curator_core/)

Have indices in Elasticsearch? This is the tool for you!

Like a museum curator manages the exhibits and collections on display,
Elasticsearch Curator helps you curate, or manage your indices.

## [Curator API Documentation](http://curator.readthedocs.org/)

Since version 2.0, Curator ships with both an API and wrapper scripts (which are
actually defined as entry points).  This allows you to write your own scripts to
accomplish similar goals, or even new and different things with the [Curator API](http://curator.readthedocs.org/),
and the [Elasticsearch Python API](http://elasticsearch-py.readthedocs.org/).

## [Curator CLI Documentation](http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html)

The Curator CLI Documentation is now a part of the document repository at
http://elastic.co/guide at http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html


## Getting Started

Install using pip

    pip install elasticsearch-curator

Run `curator --help` for usage specifics.

## [Documentation & Examples](http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html)

The documentation for the CLI is now part of the document repository at http://elastic.co/guide
at http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

The [Curator Wiki](http://github.com/elastic/curator/wiki) on Github is now a
place to add your own examples and ideas.

## Contributing

* fork the repo
* make changes in your fork
* add tests to cover your changes (if necessary)
* run tests
* sign the [CLA](http://elastic.co/contributor-agreement/)
* send a pull request!

To run from source, use the `run_curator.py` and `run_es_repo_mgr.py` scripts
in the root directory of the project.

### Running Tests

To run the test suite just run `python setup.py test`

When changing code, contributing new code or fixing a bug please make sure you
include tests in your PR (or mark it as without tests so that someone else can
pick it up to add the tests). When fixing a bug please make sure the test
actually tests the bug - it should fail without the code changes and pass after
they're applied (it can still be one commit of course).

The tests will try to connect to your local elasticsearch instance and run
integration tests against it. This will delete all the data stored there! You
can use the env variable `TEST_ES_SERVER` to point to a different instance (for
example 'otherhost:9203').

## Versioning

There are two branches for development - `master` and `0.6`. Master branch is
used to track all the changes for Elasticsearch 1.0 and beyond whereas 0.6
tracks Elasticsearch 0.90 and the corresponding `elasticsearch-py` version.

Releases with major versions greater than 1 (X.Y.Z, where X is > 1) are to be
used with Elasticsearch 1.0 and later, 0.6 releases are meant to work with
Elasticsearch 0.90.X.

## Origins

Curator was first called `clearESindices.py` [1] and was almost immediately
renamed to `logstash_index_cleaner.py` [1].  After a time it was migrated under
the [logstash](https://github.com/elastic/logstash) repository as
`expire_logs`.  Soon thereafter, Jordan Sissel was hired by Elasticsearch, as
was the original author of this tool.  It became Elasticsearch Curator after
that and is now hosted at <https://github.com/elastic/curator>

[1] <https://logstash.jira.com/browse/LOGSTASH-211>
