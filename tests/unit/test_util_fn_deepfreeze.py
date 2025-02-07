from datetime import datetime
from unittest.mock import MagicMock

import pytest

from curator.actions.deepfreeze import (
    decode_date,
    get_all_indices_in_repo,
    get_timestamp_range,
    thaw_indices,
)


def test_decode_date():
    rightnow = datetime.now()
    assert decode_date("2024-01-01") == datetime(2024, 1, 1)
    assert decode_date(rightnow) == rightnow
    with pytest.raises(ValueError):
        decode_date("not-a-date")
    with pytest.raises(ValueError):
        decode_date(123456)
    with pytest.raises(ValueError):
        decode_date(None)


def test_get_all_indices_in_repo():
    client = MagicMock()
    client.snapshot.get.return_value = {
        "snapshots": [
            {"indices": ["index1", "index2"]},
            {"indices": ["index3"]},
        ]
    }
    indices = get_all_indices_in_repo(client, "test-repo")
    indices.sort()
    assert indices == [
        "index1",
        "index2",
        "index3",
    ]


def test_get_timestamp_range():
    client = MagicMock()
    client.search.return_value = {
        "aggregations": {
            "earliest": {"value_as_string": "2025-02-01 07:46:04.57735"},
            "latest": {"value_as_string": "2025-02-06 07:46:04.57735"},
        }
    }
    earliest, latest = get_timestamp_range(client, ["index1", "index2"])
    assert earliest == datetime(2025, 2, 1, 7, 46, 4, 577350)
    assert latest == datetime(2025, 2, 6, 7, 46, 4, 577350)


def test_thaw_indices():
    client = MagicMock()
    client.get_objects.return_value = [
        {"bucket": "bucket1", "base_path": "path1", "object_keys": ["key1"]},
        {"bucket": "bucket2", "base_path": "path2", "object_keys": ["key2"]},
    ]
    thaw_indices(client, ["index1", "index2"])
    client.thaw.assert_any_call("bucket1", "path1", ["key1"], 7, "Standard")
    client.thaw.assert_any_call("bucket2", "path2", ["key2"], 7, "Standard")
