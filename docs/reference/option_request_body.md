---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/option_request_body.html
---

# request_body [option_request_body]

::::{note}
This setting is only used by the [reindex](/reference/reindex.md) action.
::::


## Manual index selection [_manual_index_selection]

The `request_body` option is the heart of the reindex action. In here, using YAML syntax, you recreate the body sent to Elasticsearch as described in [the official documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex).  You can manually select indices as with this example:

```yaml
actions:
  1:
    description: "Reindex index1 into index2"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: index1
        dest:
          index: index2
    filters:
    - filtertype: none
```

You can also select multiple indices to reindex by making a list in acceptable YAML syntax:

```yaml
actions:
  1:
    description: "Reindex index1,index2,index3 into new_index"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: ['index1', 'index2', 'index3']
        dest:
          index: new_index
    filters:
    - filtertype: none
```

::::{important}
You *must* set at least a [none](/reference/filtertype_none.md) filter, or the action will fail.  Do not worry.  If you’ve manually specified your indices, those are the only ones which will be reindexed.
::::



## Filter-Selected Indices [_filter_selected_indices]

Curator allows you to use all indices found by the `filters` section by setting the `source` index to `REINDEX_SELECTION`, like this:

```yaml
actions:
  1:
    description: >-
      Reindex all daily logstash indices from March 2017 into logstash-2017.03
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          index: REINDEX_SELECTION
        dest:
          index: logstash-2017.03
    filters:
    - filtertype: pattern
      kind: prefix
      value: logstash-2017.03.
```


## Reindex From Remote [_reindex_from_remote]

You can also reindex from remote:

```yaml
actions:
  1:
    description: "Reindex remote index1 to local index1"
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          remote:
            host: http://otherhost:9200
            username: myuser
            password: mypass
          index: index1
        dest:
          index: index1
    filters:
    - filtertype: none
```

::::{important}
You *must* set at least a [none](/reference/filtertype_none.md) filter, or the action will fail.  Do not worry.  Only the indices you specified in `source` will be reindexed.
::::


Curator will create a connection to the host specified as the `host` key in the above example.  It will determine which port to connect to, and whether to use SSL by parsing the URL entered there.  Because this `host` is specifically used by Elasticsearch, and Curator is making a separate connection, it is important to ensure that both Curator *and* your Elasticsearch cluster have access to the remote host.

If you do not whitelist the remote cluster, you will not be able to reindex. This can be done by adding the following line to your `elasticsearch.yml` file:

```yaml
reindex.remote.whitelist: remote_host_or_IP1:9200, remote_host_or_IP2:9200
```

or by adding this flag to the command-line when starting Elasticsearch:

```sh
bin/elasticsearch -Edefault.reindex.remote.whitelist="remote_host_or_IP:9200"
```

Of course, be sure to substitute the correct host, IP, or port.

Other client connection arguments can also be supplied in the form of action options:

* [remote_url_prefix](/reference/option_remote_url_prefix.md)
* [remote_certificate](/reference/option_remote_certificate.md)
* [remote_client_cert](/reference/option_remote_client_cert.md)
* [remote_client_key](/reference/option_remote_client_key.md)


## Reindex From Remote With Filter-Selected Indices [_reindex_from_remote_with_filter_selected_indices]

You can even reindex from remote with filter-selected indices on the remote side:

```yaml
actions:
  1:
    description: >-
      Reindex all remote daily logstash indices from March 2017 into local index
      logstash-2017.03
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          remote:
            host: http://otherhost:9200
            username: myuser
            password: mypass
          index: REINDEX_SELECTION
        dest:
          index: logstash-2017.03
      remote_filters:
      - filtertype: pattern
        kind: prefix
        value: logstash-2017.03.
    filters:
    - filtertype: none
```

::::{important}
Even though you are reindexing from remote, you *must* set at least a [none](/reference/filtertype_none.md) filter, or the action will fail.  Do not worry.  Only the indices specified in `source` will be reindexed.
::::


Curator will create a connection to the host specified as the `host` key in the above example.  It will determine which port to connect to, and whether to use SSL by parsing the URL entered there.  Because this `host` is specifically used by Elasticsearch, and Curator is making a separate connection, it is important to ensure that both Curator *and* your Elasticsearch cluster have access to the remote host.

If you do not whitelist the remote cluster, you will not be able to reindex. This can be done by adding the following line to your `elasticsearch.yml` file:

```yaml
reindex.remote.whitelist: remote_host_or_IP1:9200, remote_host_or_IP2:9200
```

or by adding this flag to the command-line when starting Elasticsearch:

```sh
bin/elasticsearch -Edefault.reindex.remote.whitelist="remote_host_or_IP:9200"
```

Of course, be sure to substitute the correct host, IP, or port.

Other client connection arguments can also be supplied in the form of action options:

* [remote_url_prefix](/reference/option_remote_url_prefix.md)
* [remote_certificate](/reference/option_remote_certificate.md)
* [remote_client_cert](/reference/option_remote_client_cert.md)
* [remote_client_key](/reference/option_remote_client_key.md)


## Reindex - Migration [_reindex_migration]

Curator allows reindexing, particularly from remote, as a migration path.  This can be a very useful feature for migrating an older cluster (1.4+) to a new cluster, on different hardware.  It can also be a useful tool for serially reindexing indices into newer mappings in an automatable way.

Ordinarily, a reindex operation is from either one, or many indices, to a single, named index.  Assigning the `dest` `index` to `MIGRATION` tells Curator to treat this reindex differently.

::::{important}
**If it is a *local* reindex,** you *must* set either [migration_prefix](/reference/option_migration_prefix.md), or [migration_suffix](/reference/option_migration_suffix.md), or both.  This prevents collisions and other bad things from happening.  By assigning a prefix or a suffix (or both), you can reindex any local indices to new versions of themselves, but named differently.

It is true the Reindex API already has this functionality.  Curator includes this same functionality for convenience.

::::


This example will reindex all of the remote indices matching `logstash-2017.03.` into the local cluster, but preserve the original index names, rather than merge all of the contents into a single index.  Internal to Curator, this results in multiple reindex actions: one per index.  All other available options and settings are available.

```yaml
actions:
  1:
    description: >-
      Reindex all remote daily logstash indices from March 2017 into local
      versions with the same index names.
    action: reindex
    options:
      wait_interval: 9
      max_wait: -1
      request_body:
        source:
          remote:
            host: http://otherhost:9200
            username: myuser
            password: mypass
          index: REINDEX_SELECTION
        dest:
          index: MIGRATION
      remote_filters:
      - filtertype: pattern
        kind: prefix
        value: logstash-2017.03.
    filters:
    - filtertype: none
```

::::{important}
Even though you are reindexing from remote, you *must* set at least a [none](/reference/filtertype_none.md) filter, or the action will fail.  Do not worry.  Only the indices specified in `source` will be reindexed.
::::


Curator will create a connection to the host specified as the `host` key in the above example.  It will determine which port to connect to, and whether to use SSL by parsing the URL entered there.  Because this `host` is specifically used by Elasticsearch, and Curator is making a separate connection, it is important to ensure that both Curator *and* your Elasticsearch cluster have access to the remote host.

If you do not whitelist the remote cluster, you will not be able to reindex. This can be done by adding the following line to your `elasticsearch.yml` file:

```yaml
reindex.remote.whitelist: remote_host_or_IP1:9200, remote_host_or_IP2:9200
```

or by adding this flag to the command-line when starting Elasticsearch:

```sh
bin/elasticsearch -Edefault.reindex.remote.whitelist="remote_host_or_IP:9200"
```

Of course, be sure to substitute the correct host, IP, or port.

Other client connection arguments can also be supplied in the form of action options:

* [remote_url_prefix](/reference/option_remote_url_prefix.md)
* [remote_certificate](/reference/option_remote_certificate.md)
* [remote_client_cert](/reference/option_remote_client_cert.md)
* [remote_client_key](/reference/option_remote_client_key.md)
* [migration_prefix](/reference/option_migration_prefix.md)
* [migration_suffix](/reference/option_migration_suffix.md)


## Other scenarios and options [_other_scenarios_and_options]

Nearly all scenarios supported by the reindex API are supported in the request_body, including (but not limited to):

* Pipelines
* Scripting
* Queries
* Conflict resolution
* Limiting by count
* Versioning
* Reindexing operation type (for example, create-only)

Read more about these, and more, at [http://www.elastic.co/guide/en/elasticsearch/reference/8.15/docs-reindex.html](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/docs-reindex.html)

Notable exceptions include:

* You cannot manually specify slices.  Instead, use the [slices](/reference/option_slices.md) option for automated sliced reindexing.
