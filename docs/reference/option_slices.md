---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_slices.html
---

# slices [option_slices]

::::{note}
This setting is only used by the [reindex](/reference/reindex.md) action.
::::


This setting can speed up reindexing operations by using [Sliced Scroll](elasticsearch://reference/elasticsearch/rest-apis/paginate-search-results.md#slice-scroll) to slice on the \_uid.

```yaml
actions:
  1:
    description: "Reindex index1,index2,index3 into new_index"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      slices: 3
      request_body:
        source:
          index: ['index1', 'index2', 'index3']
        dest:
          index: new_index
    filters:
    - filtertype: none
```

## Picking the number of slices [_picking_the_number_of_slices]

Here are a few recommendations around the number of `slices` to use:

* Don’t use large numbers. `500` creates fairly massive CPU thrash, so Curator will not allow a number larger than this.
* It is more efficient from a query performance standpoint to use some multiple of the number of shards in the source index.
* Using exactly as many shards as are in the source index is the most efficient from a query performance standpoint.
* Indexing performance should scale linearly across available resources with the number of slices.
* Whether indexing or query performance dominates that process depends on lots of factors like the documents being reindexed and the cluster doing the reindexing.


