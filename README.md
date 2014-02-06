# Curator

Have time-series indices in Elasticsearch? This is the tool for you!

## Usage

Install using pip

    pip install elasticsearch-curator


See `curator --help` for usage specifics.

### Defaults

The default values for host, port and prefix are:

    --host localhost
    --port 9200
    -t (or --timeout) 30
    -C (or --curation-style) time
    -T (or --time-unit) days
    -p (or --prefix) logstash-
    -s (or --separator) .
    --max_num_segments 2

If your values match these you do not need to include them.  The `prefix` should be everything before the date string.

### Examples

Close indices older than 14 days, delete indices older than 30 days (See https://github.com/elasticsearch/curator/issues/1):

    curator --host my-elasticsearch -d 30 -c 14

Keep 14 days of logs in elasticsearch:

    curator --host my-elasticsearch -d 14

Disable bloom filter for indices older than 2 days, close indices older than 14 days, delete indices older than 30 days:

    curator --host my-elasticsearch -b 2 -c 14 -d 30
    
Optimize (Lucene forceMerge) indices older than 2 days to 1 segment per shard:

    curator --host my-elasticsearch -t 3600 -o 2 --max_num_segments 1

Keep 1TB of data in elasticsearch, show debug output:

    curator --host my-elasticsearch -C space -g 1024 -D

Dry run of above:

    curator --host my-elasticsearch -C space -g 1024 -D -n

## Documentation and Errata

If you need to close and delete based on different criteria, please use separate command lines, e.g.

    curator.py --host my-elasticsearch -C space -g 1024
    curator.py --host my-elasticsearch -c 15
    
When using optimize the current behavior is to wait until the optimize operation is complete before continuing.  With large indices, this can result in timeouts with the default 30 seconds.  It is recommended that you increase the timeout to at least 3600 seconds, if not more.  


## Contributing

* fork the repo
* make changes in your fork
* send a pull request!

## Origins

<https://logstash.jira.com/browse/LOGSTASH-211>

