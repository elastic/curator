from datetime import datetime
from unittest.mock import MagicMock

import pytest
from elasticsearch.exceptions import NotFoundError

from curator.actions.deepfreeze import (
    Repository,
    check_restore_status,
    ensure_settings_index,
    get_all_indices_in_repo,
    get_settings,
    get_timestamp_range,
    push_to_glacier,
    thaw_repo,
)


def test_push_to_glacier_no_objects_found():
    s3 = MagicMock()
    repo = Repository(
        {
            "name": "test-repo",
            "bucket": "test-bucket",
            "base_path": "test-path",
            "start": "2023-01-01T00:00:00",
            "end": "2023-01-02T00:00:00",
        }
    )
    s3.list_objects_v2.return_value = {}

    push_to_glacier(s3, repo)

    s3.copy_object.assert_not_called()


def test_push_to_glacier_objects_found():
    s3 = MagicMock()
    repo = Repository(
        {
            "name": "test-repo",
            "bucket": "test-bucket",
            "base_path": "test-path",
            "start": "2023-01-01T00:00:00",
            "end": "2023-01-02T00:00:00",
        }
    )
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "object1"},
            {"Key": "object2"},
        ]
    }

    push_to_glacier(s3, repo)

    assert s3.copy_object.call_count == 2
    s3.copy_object.assert_any_call(
        Bucket="test-bucket",
        Key="object1",
        CopySource={"Bucket": "test-bucket", "Key": "object1"},
        StorageClass="GLACIER",
    )
    s3.copy_object.assert_any_call(
        Bucket="test-bucket",
        Key="object2",
        CopySource={"Bucket": "test-bucket", "Key": "object2"},
        StorageClass="GLACIER",
    )


def test_check_restore_status_no_objects_found():
    s3 = MagicMock()
    repo = Repository(
        {
            "name": "test-repo",
            "bucket": "test-bucket",
            "base_path": "test-path",
            "start": "2023-01-01T00:00:00",
            "end": "2023-01-02T00:00:00",
        }
    )
    s3.list_objects_v2.return_value = {}

    result = check_restore_status(s3, repo)

    assert result is None
    s3.list_objects_v2.assert_called_once_with(Bucket="test-bucket", Prefix="test-path")


def test_check_restore_status_objects_restored():
    s3 = MagicMock()
    repo = Repository(
        {
            "name": "test-repo",
            "bucket": "test-bucket",
            "base_path": "test-path",
            "start": "2023-01-01T00:00:00",
            "end": "2023-01-02T00:00:00",
        }
    )
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "object1"},
            {"Key": "object2"},
        ]
    }
    s3.head_object.side_effect = [
        {"Restore": 'ongoing-request="false"'},
        {"Restore": 'ongoing-request="false"'},
    ]

    result = check_restore_status(s3, repo)

    assert result is True
    s3.list_objects_v2.assert_called_once_with(Bucket="test-bucket", Prefix="test-path")
    assert s3.head_object.call_count == 2


def test_check_restore_status_objects_still_restoring():
    s3 = MagicMock()
    repo = Repository(
        {
            "name": "test-repo",
            "bucket": "test-bucket",
            "base_path": "test-path",
            "start": "2023-01-01T00:00:00",
            "end": "2023-01-02T00:00:00",
        }
    )
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "object1"},
            {"Key": "object2"},
        ]
    }
    s3.head_object.side_effect = [
        {"Restore": 'ongoing-request="true"'},
        {"Restore": 'ongoing-request="false"'},
    ]

    result = check_restore_status(s3, repo)

    assert result is False
    s3.list_objects_v2.assert_called_once_with(Bucket="test-bucket", Prefix="test-path")
    assert s3.head_object.call_count == 1


def test_check_restore_status_no_restore_header():
    s3 = MagicMock()
    repo = Repository(
        {
            "name": "test-repo",
            "bucket": "test-bucket",
            "base_path": "test-path",
            "start": "2023-01-01T00:00:00",
            "end": "2023-01-02T00:00:00",
        }
    )
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "object1"},
            {"Key": "object2"},
        ]
    }
    s3.head_object.side_effect = [
        {"Restore": None},
        {"Restore": 'ongoing-request="false"'},
    ]

    result = check_restore_status(s3, repo)

    assert result is None
    s3.list_objects_v2.assert_called_once_with(Bucket="test-bucket", Prefix="test-path")
    assert s3.head_object.call_count == 1


def test_check_restore_status_exception():
    s3 = MagicMock()
    repo = Repository(
        {
            "name": "test-repo",
            "bucket": "test-bucket",
            "base_path": "test-path",
            "start": "2023-01-01T00:00:00",
            "end": "2023-01-02T00:00:00",
        }
    )
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "object1"},
            {"Key": "object2"},
        ]
    }
    s3.head_object.side_effect = Exception("Some error")

    result = check_restore_status(s3, repo)

    assert result is None
    s3.list_objects_v2.assert_called_once_with(Bucket="test-bucket", Prefix="test-path")
    assert s3.head_object.call_count == 1


def test_thaw_repo_no_objects_found():
    s3 = MagicMock()
    bucket_name = "test-bucket"
    base_path = "test-path"
    s3.list_objects_v2.return_value = {}

    thaw_repo(s3, bucket_name, base_path)

    s3.list_objects_v2.assert_called_once_with(Bucket=bucket_name, Prefix=base_path)
    s3.restore_object.assert_not_called()


def test_thaw_repo_objects_found():
    s3 = MagicMock()
    bucket_name = "test-bucket"
    base_path = "test-path"
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "object1"},
            {"Key": "object2"},
        ]
    }

    thaw_repo(s3, bucket_name, base_path)

    s3.list_objects_v2.assert_called_once_with(Bucket=bucket_name, Prefix=base_path)
    assert s3.restore_object.call_count == 2
    s3.restore_object.assert_any_call(
        Bucket=bucket_name,
        Key="object1",
        RestoreRequest={
            "Days": 7,
            "GlacierJobParameters": {"Tier": "Standard"},
        },
    )
    s3.restore_object.assert_any_call(
        Bucket=bucket_name,
        Key="object2",
        RestoreRequest={
            "Days": 7,
            "GlacierJobParameters": {"Tier": "Standard"},
        },
    )


def test_thaw_repo_custom_restore_days_and_tier():
    s3 = MagicMock()
    bucket_name = "test-bucket"
    base_path = "test-path"
    restore_days = 10
    retrieval_tier = "Expedited"
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "object1"},
            {"Key": "object2"},
        ]
    }

    thaw_repo(s3, bucket_name, base_path, restore_days, retrieval_tier)

    s3.list_objects_v2.assert_called_once_with(Bucket=bucket_name, Prefix=base_path)
    assert s3.restore_object.call_count == 2
    s3.restore_object.assert_any_call(
        Bucket=bucket_name,
        Key="object1",
        RestoreRequest={
            "Days": restore_days,
            "GlacierJobParameters": {"Tier": retrieval_tier},
        },
    )
    s3.restore_object.assert_any_call(
        Bucket=bucket_name,
        Key="object2",
        RestoreRequest={
            "Days": restore_days,
            "GlacierJobParameters": {"Tier": retrieval_tier},
        },
    )


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


def test_ensure_settings_index_exists():
    client = MagicMock()
    client.indices.exists.return_value = True

    ensure_settings_index(client)

    client.indices.exists.assert_called_once_with(index="deepfreeze-status")
    client.indices.create.assert_not_called()


def test_ensure_settings_index_does_not_exist():
    client = MagicMock()
    client.indices.exists.return_value = False

    ensure_settings_index(client)

    client.indices.exists.assert_called_once_with(index="deepfreeze-status")
    client.indices.create.assert_called_once_with(index="deepfreeze-status")


def test_get_settings_document_found():
    client = MagicMock()
    client.get.return_value = {
        "_source": {
            "doctype": "settings",
            "repo_name_prefix": "deepfreeze",
            "bucket_name_prefix": "deepfreeze",
            "base_path_prefix": "snapshots",
            "canned_acl": "private",
            "storage_class": "intelligent_tiering",
            "provider": "aws",
            "rotate_by": "path",
            "style": "oneup",
            "last_suffix": "000001",
        }
    }

    settings = get_settings(client)

    assert settings.repo_name_prefix == "deepfreeze"
    assert settings.bucket_name_prefix == "deepfreeze"
    assert settings.base_path_prefix == "snapshots"
    assert settings.canned_acl == "private"
    assert settings.storage_class == "intelligent_tiering"
    assert settings.provider == "aws"
    assert settings.rotate_by == "path"
    assert settings.style == "oneup"
    assert settings.last_suffix == "000001"
    client.get.assert_called_once_with(index="deepfreeze-status", id="1")


def test_get_settings_document_not_found():
    client = MagicMock()
    client.get.side_effect = NotFoundError(404, "Not Found", {})

    with pytest.raises(NotFoundError):
        get_settings(client)
