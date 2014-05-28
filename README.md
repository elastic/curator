# Curator

Have time-series indices in Elasticsearch? This is the tool for you!

## Versioning

There are two branches for development - `master` and `0.6`. Master branch is
used to track all the changes for Elasticsearch 1.0 and beyond whereas 0.6
tracks Elasticsearch 0.90 and the corresponding `elasticsearch-py` version.

Releases with major version 1 (1.X.Y) are to be used with Elasticsearch 1.0 and
later, 0.6 releases are meant to work with Elasticsearch 0.90.X.

## Usage

Install using pip

    pip install elasticsearch-curator

See `curator --help` for usage specifics.

### Defaults

The default values for the following are:

    --host localhost
    --port 9200
    -t (or --timeout) 30
    -C (or --curation-style) time
    -T (or --time-unit) days
    -p (or --prefix) logstash-
    -s (or --separator) .
    --max_num_segments 2

If your values match these you do not need to include them.  The value of `prefix` should be everything before the date string, i.e. `--prefix .marvel-` would match index `.marvel-2014.05.27`, and all other indices beginning with `.marvel-` (don't forget the trailing hyphen!).

### Examples

#### Delete indices

Delete indices older than 30 days:

    curator --host my-elasticsearch --delete 30
    
You can also use `-d` in place of `--delete`

#### Close indices

Close indices older than 14 days:

    curator --host my-elasticsearch --close 14

You can also use `-c` in place of `--close`

#### Disable bloom filter for indices

Disable bloom filter for indices older than 1:

    curator --host my-elasticsearch --bloom 1

You can also use `-b` in place of `--bloom`

#### Optimize (Lucene forceMerge) indices

*Optimize* is a bit of a misnomer.  It is in actuality a Lucene forceMerge operation. With time-series data in a per-day index, Lucene does a good job of keeping the number of segments low.  However, if no new data is being ingested, no further segment merging will happen.  There are some minor performance benefits from merging segments down to a smaller count, but a greater benefit when it comes to restarts [e.g. version upgrades, etc.] after a shutdown: with fewer segments to have to validate, the cluster comes back up sooner.

Optimize (Lucene forceMerge) indices older than 2 days to 2 segments per shard (the default is 2):

    curator --host my-elasticsearch --timeout 7200 --optimize 2

Optimize (Lucene forceMerge) indices older than 2 days to 1 segment per shard:

	curator --host my-elasticsearch --timeout 7200 --optimize 2 --max_num_segments 1
	
You can also use `-t` in place of `--timeout`, and `-o` in place of `--optimize`

Please note that `--timeout 7200` is specified in each case.  Since the optimize operation can take a long time, curator may disconnect and fail to continue with further operations if the timeout is not set high enough.  This number may need to be higher, or could be reduced depending on your scenario.  The log file will tell you how long it took to perform previous operations, which you could use as a guideline.

#### Delete by space

Keep 1024GB (1TB) of data in elasticsearch:

    curator --host my-elasticsearch --curation-style space --disk-space 1024

You can also use `-C` in place of `--curation-style`, and `-g` in place of `--disk-space`.

Note that when using size to determine which indices to keep, having closed
indices will cause inaccuracies since they cannot be added to the overall size.
This is only an issue if you have closed some indices that are not your oldest
ones.

#### Shard/index allocation

You can use curator to apply routing tags to your indices.  This is useful for migrating stale indices from your heavy-duty indexing boxes to slower-hardware search boxes.  Read more [here](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-cluster.html#allocation-filtering) about the `index.routing.allocation.require.*` settings.  In order for the index-level settings to work, you must also have corresponding node-level settings.

Apply setting `index.routing.allocation.tag=done_indexing` to indices older than 2 days:

	curator --host my-elasticsearch --require 2 --require_rule tag=done_indexing
	
You can also use `-r` in place of `--require`

#### Display indices matching prefix

Display a list of all indices matching `PREFIX` (`logstash-` by default):

	curator --host my-elasticsearch --show-indices

####  Other common flags

* `--dry-run` Will not actually perform actions, but will log the attempts.  Can also use `-n`.
* `--debug` Enables debug logging.  Can also use `-D`.
* `--loglevel` Specify desired log level. `INFO` by default.  Can also use `-ll`.
* `--logfile` Path to desired log file (STDOUT by default).  Can also use `-l`.

### Argument combining

#### Curaton styles

You cannot mix curation styles within the same command-line.  If you need to both optimize and delete by space, please use separate command lines, e.g.

    curator --host my-elasticsearch --curation-style space --disk-space 1024
    curator --host my-elasticsearch [--curation-style time] --optimize 1

#### Order of operations.

There is a specific order of operations.  The reasoning being, why disable bloom filters or close an index I'm about to delete anyway?  I should delete first, then perform those operations on remaining indices.  Likewise, in some cases you'd want to optimize an index before re-routing it (though perhaps not in others).  

This is the order of operations:

   0. Show (indices, repositories, snapshots) [exit immediately after showing]
   1. Delete Repository
   1. Create Repository
   1. Delete (by space)
   1. Delete
   1. Close
   1. Disable bloom filter
   1. Optimize
   1. Required routing (`--require`)
   1. Delete snapshot
   1. Create snapshot

#### Example of combined arguments
As a result, I can combine some operations into a single command-line:

	curator --host my-elasticsearch --timeout 3600 -c 5 -b 1 -o 1 -d 6 -l /usr/local/var/log/curator.log

This line will close all indices older than 5 days, disable bloom filter caches for indices older than 1 day, optimize indices older than 1 day, and delete indices older than 6 days.  However, it will process them in the order of operations described above, i.e. delete, close, disable bloom filters, then optimize.

## Snapshots

Snapshots have been available since Elasticsearch 1.0.  They're also in curator, starting with version 1.1.0
Read more about Elasticsearch's snapshot API [here](http://www.elasticsearch.org/guide/en/elasticsearch/reference/master/modules-snapshots.html).

A repository must be created before snapshots can be performed.  It is possible to create a repository _and_ perform a snapshot in a single command-line.  Once a repository is created, however, it only need be referenced by name.

Curator will save one index per snapshot.  It will name the snapshot the same name as the index stored.  

Snapshots can take a very long time to complete, if the index is large enough, or if you have a slow pipeline (or disks).  As a result, it is advised that you set your `--timeout` to a _very_ high number to start out with, especially if you are acting on a large volume of indices on your first run.  If a timeout condition occurs, the snapshot currently being performed will continue, but snapshots for all subsequent indices will fail.  This will appear in the log output.

### Common Repository Flags

* `--disable-compression`   Turns off compression of the snapshot files (enabled by default).
* `--chunk-size`    Big files can be broken down into chunks during snapshotting if needed. The chunk size can be specified in bytes or by using size value notation, i.e. 1g, 10m, 5k. Defaults to `null` (unlimited chunk size).
* `--max_restore_bytes_per_sec` Throttles _per node_ restore rate. Defaults to 20mb per second.
* `--max_snapshot_bytes_per_sec` Throttles _per node_ snapshot rate. Defaults to 20mb per second.


### Create a Repository (filesystem)

Curator allows you to send your [snapshot data to a filesystem](http://www.elasticsearch.org/guide/en/elasticsearch/reference/master/modules-snapshots.html#_shared_file_system_repository).  Note: *The path specified in the `location` parameter should point to the same location in the shared filesystem and be accessible on all data and master nodes.*

#### Filesystem Repository Flags

* `--repo-type`  (OPTIONAL) Defaults to `fs`
* `--location`   (REQUIRED) Path to the shared filesystem

#### Example Filesystem Repository Creation

    curator --host my-elasticsearch --create-repo --repo-type fs --repository REPOSITORY_NAME --location /path/to/repository

### Create a Repository (S3)

Curator also allows you to send your [snapshot data to an S3 bucket](https://github.com/elasticsearch/elasticsearch-cloud-aws#s3-repository).
You must have the correct version for your platform of [AWS Cloud Plugin for Elasticsearch](https://github.com/elasticsearch/elasticsearch-cloud-aws#aws-cloud-plugin-for-elasticsearch) installed for the S3 repository type to function.

#### S3 Repository Flags

* `--repo-type`  (REQUIRED) Must be: `s3` (Default is `fs`)
* `--bucket`     (REQUIRED) Repository bucket name
* `--region`     (OPTIONAL) S3 region. Defaults to `US Standard`
* `--base_path`  (OPTIONAL) S3 base path. Defaults to the bucket root directory.
* `--concurrent_streams`    (OPTIONAL) Throttles the number of streams _per node_ preforming snapshot operation. Defaults to `5`.
* `--access_key` (OPTIONAL) S3 access key. Defaults to value of `cloud.aws.access_key` in your `elasticsearch.yml`, if defined.
* `--secret_key` (OPTIONAL) S3 secret key. Defaults to value of `cloud.aws.secret_key` in your `elasticsearch.yml`, if defined.

#### Example S3 Repository Creation

    curator --host my-elasticsearch --create-repo --repo-type s3 --repository REPOSITORY_NAME --bucket MYBUCKET --access_key ACCESS_KEY --secret_key SECRET_KEY

### Snapshot Flags

* `--snap-older` Take a snapshot of indices older than `SNAP_OLDER` `TIME_UNIT`s.
* `--snap-latest` Take a snapshot of the `SNAP_LATEST` most recent number of indices matching `PREFIX`.
* `--delete-snaps` Delete snapshots older than `DELETE_SNAPS` `TIME_UNIT`s.
* `--no_wait_for_completion` Do not wait until complete to return. Waits by default. WARNING: Using this flag will cause a failure if you are backing up more than one index during a single run.
* `--ignore_unavailable` Ignore unavailable shards/indices (Default=False)
* `--include_global_state` Store cluster global state with snapshot (Default=False)
* `--partial` Do not fail if primary shard is unavailable. (Will fail by default)

#### Example Snapshot Commands

Snapshot all `logstash-YYYY.MM.dd` indices older than 1 day:

    curator --host my-elasticsearch --timeout 86400 --repository REPOSITORY_NAME --snap-older 1

Delete snapshots older than 1 year:

    curator --host my-elasticsearch --timeout 3600 --repository REPOSITORY_NAME --delete-snaps 365

Capture the most recent 3 days of Elasticsearch Marvel indices (could be used to forward to Elasticsearch support)

    curator --host my-elasticsearch --timeout 10800 --repository REPOSITORY_NAME --snap-latest 3 --prefix .marvel-

#### Extra Snapshot Commands

Show all repositories:

	curator --host my-elasticsearch --repository REPOSITORY --show-repositories

Show all snapshots matching `PREFIX`:

	curator --host my-elasticsearch --repository REPOSITORY --prefix .marvel- --show-snapshots

## Errata

### Mutually exclusive arguments

If you need to perform operations based on differing `--curation-style`s, please use separate command lines, e.g.

    curator --host my-elasticsearch --curation-style space --disk-space 1024
    curator --host my-elasticsearch [--curation-style time] --optimize 1
    
### Timeouts
With some operations (e.g. `--optimize` and `--snap-older`) the default behavior is to wait until the operation is complete before proceeding with the next step.  Since these operations can take quite a long time it is advisable to set `--timeout` to a high value (e.g. a minimum of `3600` [1 hour] for optimize operations).


## Contributing

* fork the repo
* make changes in your fork
* run tests
* send a pull request!

### Running tests

To run the test suite just run `python setup.py test`.

When changing code, contributing new code or fixing a bug please make sure you
include tests in your PR (or mark it as without tests so that someone else can
pick it up to add the tests). When fixing a bug please make sure the test
actually tests the bug - it should fail without the code changes and pass after
they're applied (it can still be one commit of course).

The tests will try to connect to your local elasticsearch instance and run
integration tests against it. This will delete all the data stored there! You
can use the env variable `TEST_ES_SERVER` to point to a different instance (for
example 'otherhost:9203').

The repository tests all expect to run on a single local node.  These tests will fail if run against a cluster due to the unhappy mixup between unit tests and shared filesystems (total cleanup afterwards).  It is possible, but you would have to manually replace `/tmp/REPOSITORY_LOCATION` with a path on your shared filesystem.  This is defined in `test_curator/integration/__init__.py`

## Origins

Curator was first called `clearESindices.py` [1] and was almost immediately renamed to `logstash_index_cleaner.py` [1].  After a time it was migrated under the [logstash](https://github.com/elasticsearch/logstash) repository as `expire_logs`.  Soon thereafter, Jordan Sissel was hired by Elasticsearch, as was the original author of this tool.  It became Elasticsearch Curator after that and is now hosted at <https://github.com/elasticsearch/curator>

[1] <https://logstash.jira.com/browse/LOGSTASH-211>

