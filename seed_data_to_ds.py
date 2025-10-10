#!/usr/bin/env python3

import time
from datetime import datetime

from elasticsearch import Elasticsearch, NotFoundError

# Configuration
ES_HOST = "https://es-test.bwortman.us"  # Change if needed
DATASTREAM_NAME = "test_datastream"
ES_USERNAME = "bret"
ES_PASSWORD = "2xqT2IO1OQ%tfMHP"

# Initialize Elasticsearch client with authentication
es = Elasticsearch(ES_HOST, basic_auth=(ES_USERNAME, ES_PASSWORD))


def create_index_template(es, alias_name):
    """Creates an index template with a rollover alias."""
    template_body = {
        "index_patterns": [f"{alias_name}-*"],
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "aliases": {alias_name: {"is_write_index": True}},
    }
    es.indices.put_template(name=alias_name, body=template_body)


def create_initial_index(es, alias_name):
    """Creates the initial index for rollover if it doesn't exist."""
    first_index = f"{alias_name}-000001"
    try:
        if not es.indices.exists(index=first_index):
            es.indices.create(
                index=first_index,
                body={"aliases": {alias_name: {"is_write_index": True}}},
            )
    except NotFoundError:
        print(f"Index {first_index} not found, creating a new one.")
        es.indices.create(
            index=first_index, body={"aliases": {alias_name: {"is_write_index": True}}}
        )


# Ensure the index template and initial index exist
create_index_template(es, DATASTREAM_NAME)
create_initial_index(es, DATASTREAM_NAME)

while True:
    document = {
        "timestamp": datetime.utcnow().isoformat(),
        "message": "Hello, Elasticsearch!",
    }

    es.index(index=DATASTREAM_NAME, document=document)
    # print(f"Indexed document: {document}")

    # Perform rollover if conditions are met
    try:
        es.indices.rollover(
            alias=DATASTREAM_NAME, body={"conditions": {"max_docs": 1000}}
        )
    except NotFoundError:
        print("Rollover failed: Alias not found. Ensure the initial index is created.")

    time.sleep(1)
