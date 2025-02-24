#!/bin/bash

# Prompt for S3 credentials (silent input for security)
read -sp "Enter S3 Access Key: " ACCESS_KEY
echo
read -sp "Enter S3 Secret Key: " SECRET_KEY
echo
read -p "Enter Elasticsearch version: " VERSION
echo

# Get a list of running Elasticsearch container IDs
CONTAINERS=$(docker ps --filter "ancestor=curator_estest:${VERSION}" --format "{{.ID}}")

if [ -z "$CONTAINERS" ]; then
    echo "No running Elasticsearch containers found."
    exit 1
fi

# Loop through each container and set the credentials
for CONTAINER in $CONTAINERS; do
    echo "Setting credentials in container $CONTAINER..."
    echo "$ACCESS_KEY" | docker exec -i "$CONTAINER" bin/elasticsearch-keystore add s3.client.default.access_key --stdin
    echo "$SECRET_KEY" | docker exec -i "$CONTAINER" bin/elasticsearch-keystore add s3.client.default.secret_key --stdin
    docker restart "$CONTAINER"
    echo "Restarted container $CONTAINER."
done

echo "S3 credentials have been set in all Elasticsearch containers."

echo "Adding enterprise license"
if [[ -f license.json ]]; then
    curl -X PUT "http://localhost:9200/_license" \
         -H "Content-Type: application/json" \
         -d @license-release-stack-enterprise.json
else
    curl -X POST "http://localhost:9200/_license/start_trial?acknowledge=true"
fi
