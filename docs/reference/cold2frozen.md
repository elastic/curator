---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/cold2frozen.html
---

# Cold2Frozen [cold2frozen]

::::{important}
This action is for an unusual case where an index is a mounted, searchable snapshot in the cold tier and is not associated with an ILM policy. This action will not work with an index associated with an ILM policy regardless of the value of `allow_ilm_indices`.
::::


```yaml
action: cold2frozen
description: "Migrate non-ILM indices from the cold tier to the frozen tier"
options:
  index_settings: {}
  ignore_index_settings: []
  wait_for_completion: True
filters:
- filtertype: ...
```

::::{note}
Empty values and commented lines will result in the default value, if any, being selected.  If a setting is set, but not used by a given action, it will be ignored.
::::


This action migrates the selected non-ILM indices from the cold tier to the frozen tier. You may well ask why this action is here and why it is limited to non-ILM indices. The answer is "redacted data." If an index must be restored from the cold tier to be live so that sensitive data can be redacted, at present, it must be disassociated from an ILM policy to accomplish this. If you forcemerge and re-snapshot the redacted index, you can still put it in the cold or frozen tier, but it will not be associated with an ILM policy any more. This custom action is for moving that manually re-mounted cold tier index to the frozen tier, preserving the aliases it currently has.

## index_settings [_index_settings]

Settings that should be added to the index when it is mounted. This should be a YAML dictionary containing anything under what would normally appear in `settings`.

See the [Elasticsearch Searchable snapshots API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-searchable-snapshots-mount).

```yaml
action: cold2frozen
description: "Migrate non-ILM indices from the cold tier to the frozen tier"
options:
  index_settings:
    routing:
      allocation:
        include:
          _tier_preference: data_frozen
  ignore_index_settings: []
  wait_for_completion: True
filters:
- filtertype: ...
```

::::{note}
If unset, the default behavior is to ensure that the `_tier_preference` is `data_frozen`, if available. If it is not, Curator will assess which data tiers are available in your cluster and use those from coldest to warmest, e.g. `data_cold,data_warm,data_hot`. If none of these are available, it will default to `data_content`.
::::



## ignore_index_settings [_ignore_index_settings]

This should be a YAML list of index settings the migrated index should ignore after mount.

See the [Elasticsearch Searchable snapshots API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-searchable-snapshots-mount).

```yaml
action: cold2frozen
description: "Migrate non-ILM indices from the cold tier to the frozen tier"
options:
  index_settings:
  ignore_index_settings:
    - 'index.refresh_interval'
  wait_for_completion: True
filters:
- filtertype: ...
```

::::{note}
If unset, the default behavior is to ensure that the `index.refresh_interval` is ignored.
::::



## Optional settings [_optional_settings_5]

* [search_pattern](/reference/option_search_pattern.md)
* [wait_for_completion](/reference/option_wfc.md)
* [ignore_empty_list](/reference/option_ignore_empty.md)
* [timeout_override](/reference/option_timeout_override.md)
* [continue_if_exception](/reference/option_continue.md)
* [disable_action](/reference/option_disable.md)


