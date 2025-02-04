#!/usr/bin/env python3

import argparse

from elasticsearch import Elasticsearch


def get_snapshot_indices(es, repository):
    """Retrieve all indices from snapshots in the given repository."""
    snapshots = es.snapshot.get(repository=repository, snapshot="_all")
    indices = set()

    for snapshot in snapshots["snapshots"]:
        indices.update(snapshot["indices"])

    return list(indices)


def get_timestamp_range(es, indices):
    """Determine the earliest and latest @timestamp values from the given indices."""
    query = {
        "size": 0,
        "aggs": {
            "earliest": {"min": {"field": "@timestamp"}},
            "latest": {"max": {"field": "@timestamp"}},
        },
    }

    response = es.search(index=",".join(indices), body=query)

    earliest = response["aggregations"]["earliest"]["value_as_string"]
    latest = response["aggregations"]["latest"]["value_as_string"]

    return earliest, latest


def main():
    parser = argparse.ArgumentParser(
        description="Find earliest and latest @timestamp from snapshot indices."
    )
    parser.add_argument(
        "--host", default="https://elasticsearch.bwortman.us", help="Elasticsearch host"
    )
    parser.add_argument("--repository", required=True, help="Snapshot repository name")
    parser.add_argument("--username", required=True, help="Elasticsearch username")
    parser.add_argument("--password", required=True, help="Elasticsearch password")

    args = parser.parse_args()

    es = Elasticsearch(args.host, basic_auth=(args.username, args.password))

    indices = get_snapshot_indices(es, args.repository)
    if not indices:
        print("No indices found in the snapshots.")
        return

    earliest, latest = get_timestamp_range(es, indices)

    print(f"Earliest @timestamp: {earliest}")
    print(f"Latest @timestamp: {latest}")


if __name__ == "__main__":
    main()
