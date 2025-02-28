---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/faq_partial_delete.html
---

# Q: Can I delete only certain data from within indices? [faq_partial_delete]

## A: It’s complicated [_a_its_complicated]


#### TL;DR: No. Curator can only delete entire indices. [_tldr_no_curator_can_only_delete_entire_indices]


#### Full answer: [_full_answer]

As a thought exercise, think of Elasticsearch indices as being like databases, or tablespaces within a database. If you had hundreds of millions of rows to delete from your database, would you run a separate `DELETE from TABLE where date<YYYY.MM.dd` to assemble hundreds of millions of individual delete operations every day, or would you partition your tables in a way that you could simply run `DROP table TABLENAME.YYYY.MM.dd`? The strain on your database would be astronomical on the former and next to nothing on the latter. Elasticsearch works much the same way. While Elasticsearch *can* technically do both methods, for use-cases with time-series data (like logging), we recommend dropping entire indices vs. the extremely I/O expensive search and delete method. Curator was created to help fill that need.

While you can store different types within different indices (e.g. syslog-2014.05.05, apache-2015.05.06), this gets very expensive, very quickly in a totally different way. Each shard in Elasticsearch is a Lucene index. Each index requires a portion of the heap to exist and be kept current. If you have 3 daily indices with 5 primary shards each, you suddenly have reduced the available heap space for shard management by a factor of 3, having gone from 5 shards to 15, *per index,* not counting multiple indexes per day. The ways to mitigate this (if you pursue this route) include massive daily indexing boxes and using shard allocation/routing to move indices to specific members of the cluster where they can have less effect; keeping fewer days of information; having more nodes in your cluster, and so forth.


#### Conclusion: [_conclusion]

While it may be desirable to have different life-cycles for your data, sometimes it’s just easier and cheaper to store everything as long as the longest life-cycle you wish to maintain.


#### Post-script: [_post_script]

Even though it is neither recommended <sup class="footnote">[<a id="_footnoteref_1" class="footnote" href="#_footnotedef_1" title="View footnote.">1</a>]</sup>(http://blog.mikemccandless.com/2011/02/visualizing-lucenes-segment-merges.md) and watch what happens to your segments when you delete data.], nor best practices, it is still possible to perform these search & delete operations yourself, using the [Delete-by-Query API](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/docs-delete-by-query.md). Curator will not be modified to perform operations such as these, however. Curator is meant to manage at the index level, rather than the data level.

<hr>

