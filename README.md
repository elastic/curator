# Expire logs!

Need to delete old data in logstash/elasticsearch? This is the tool for you!

## Usage

Install dependencies

    pip install -r requirements.txt

See `python logstash_index_cleaner.py --help` for specifics, but here are some examples:

Keep 14 days of logs in elasticsearch:

    python logstash_index_cleaner.py --host my-elasticsearch -d 14

Keep 1TB of data in elasticsearch, show debug output:

    python logstash_index_cleaner.py --host my-elasticsearch -g 1024 -l debug

Dry run of above:

    python logstash_index_cleaner.py --host my-elasticsearch -g 1024 -l debug -n

## Contributing

* fork the repo
* make changes in your fork
* send a pull request!

## Origins

<https://logstash.jira.com/browse/LOGSTASH-211>

This tool was written by by Aaron Mildenstein, Njal Karevoll, and François
Deppierraz.

