# Expire logs!

Need to delete old data in logstash/elasticsearch? This is the tool for you!

## Usage

Install dependencies

    pip install -r requirements.txt


See `python logstash_index_cleaner.py --help` for usage specifics.

### Defaults

The default values for host, port and prefix are:

    --host localhost
    --port 9200
    -p (or --prefix) logstash-

If your values match these you do not need to include them.  The `prefix` should be everything before the date string.

### Examples

Close indices older than 14 days, delete indices older than 30 days (See https://github.com/logstash/expire-logs/issues/1):

    python logstash_index_cleaner.py --host my-elasticsearch -d 30 --keep-open-days 14

Keep 14 days of logs in elasticsearch:

    python logstash_index_cleaner.py --host my-elasticsearch -d 14

Keep 1TB of data in elasticsearch, show debug output:

    python logstash_index_cleaner.py --host my-elasticsearch -g 1024 -l debug

Dry run of above:

    python logstash_index_cleaner.py --host my-elasticsearch -g 1024 -l debug -n

## Documentation and Errata

Be sure not to mix types in the same command-line when using both close and delete operations, e.g.

    DO NOT DO THIS: python logstash_index_cleaner.py --host my-elasticsearch -d 30 --keep-open-size 300

If you need to close and delete based on different criteria, please use separate command lines, e.g.

    python logstash_index_cleaner.py --host my-elasticsearch --keep-open-size 300
    python logstash_index_cleaner.py --host my-elasticsearch -d 30


## Contributing

* fork the repo
* make changes in your fork
* send a pull request!

## Origins

<https://logstash.jira.com/browse/LOGSTASH-211>

This tool was written by by Aaron Mildenstein, Njal Karevoll, and François
Deppierraz.

