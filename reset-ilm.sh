#!/bin/bash

ELASTICSEARCH_HOST="elasticsearch.bwortman.us"
curl -sku bret:2xqT2IO1OQ%tfMHP -X PUT -H "Content-Type: application/json" -d '{"policy":{"phases":{"hot":{"min_age":"0ms","actions":{"rollover":{"max_age":"1m","max_primary_shard_size":"40gb"},"set_priority":{"priority":100}}},"frozen":{"min_age":"5m","actions":{"searchable_snapshot":{"snapshot_repository":"df-eah-test-000001","force_merge_index":true}}},"delete":{"min_age":"10m","actions":{"delete":{"delete_searchable_snapshot":false}}}}}}' "https://$ELASTICSEARCH_HOST/_ilm/policy/deepfreeze-policy"
echo
