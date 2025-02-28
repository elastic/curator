---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/about-origin.html
---

# Origin [about-origin]

Curator was first called [`clearESindices.py`](https://logstash.jira.com/browse/LOGSTASH-211).  Its sole function was to delete indices. It was almost immediately renamed to [`logstash_index_cleaner.py`](https://logstash.jira.com/browse/LOGSTASH-211). After a time it was briefly relocated under the [logstash](https://github.com/elastic/logstash) repository as `expire_logs`, at which point it began to gain new functionality.  Soon thereafter, Jordan Sissel was hired by Elastic (then still Elasticsearch), as was the original author of Curator.  Not long after that it became Elasticsearch Curator and is now hosted at [https://github.com/elastic/curator](https://github.com/elastic/curator)

Curator now performs many operations on your Elasticsearch indices, from delete to snapshot to shard allocation routing.

