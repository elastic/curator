#!/bin/bash

# Elasticsearch host
ES_HOST="192.168.10.31:9200"

echo "Removing status index"
curl -sku bret:2xqT2IO1OQ%tfMHP -X DELETE "https://$ES_HOST/deepfreeze-status"

echo "Removing testing datastream"
curl -sku bret:2xqT2IO1OQ%tfMHP -X DELETE "https://$ES_HOST/_data_stream/deepfreeze-testing"

# Pattern for repository names (e.g., backup_*)
PATTERN="df-eah-test-*"

# Get list of all snapshot repositories
REPOS=$(curl -sku bret:2xqT2IO1OQ%tfMHP -X GET "https://$ES_HOST/_snapshot/_all" | jq -r 'keys[]')

echo "Removing repositories matching $PATTERN"
# Loop through repositories and delete those matching the pattern
for REPO in $REPOS; do
    if [[ $REPO == $PATTERN ]]; then
        echo "Deleting repository: $REPO"
        curl -sku bret:2xqT2IO1OQ%tfMHP -X DELETE "https://$ES_HOST/_snapshot/$REPO"
        echo "Deleted $REPO"
    fi
done

echo "Removing bucket contents"
aws s3 rm s3://bdw-eah-test --recursive

echo "Removing bucket"
aws s3api delete-bucket --bucket bdw-eah-test
