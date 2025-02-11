from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from elasticsearch8 import Elasticsearch, NotFoundError

from curator.actions.deepfreeze import (
    SETTINGS_ID,
    STATUS_INDEX,
    Repository,
    Settings,
    create_repo,
    decode_date,
    ensure_settings_index,
    get_next_suffix,
    get_repos,
    get_settings,
    get_unmounted_repos,
    save_settings,
    unmount_repo,
)
from curator.exceptions import ActionError


def test_save_settings_document_exists():
    client = MagicMock(spec=Elasticsearch)
    client.snapshot = MagicMock()
    settings = Settings()
    client.get.return_value = {"_source": settings.__dict__}

    save_settings(client, settings)

    client.get.assert_called_once_with(index=STATUS_INDEX, id=SETTINGS_ID)
    client.update.assert_called_once_with(
        index=STATUS_INDEX, id=SETTINGS_ID, doc=settings.__dict__
    )


def test_save_settings_document_does_not_exist():
    client = MagicMock(spec=Elasticsearch)
    settings = Settings()
    client.get.side_effect = NotFoundError(404, "Not Found", {})

    save_settings(client, settings)

    client.get.assert_called_once_with(index=STATUS_INDEX, id=SETTINGS_ID)
    client.create.assert_called_once_with(
        index=STATUS_INDEX, id=SETTINGS_ID, document=settings.__dict__
    )


def test_ensure_settings_index_exists():
    client = MagicMock(spec=Elasticsearch)
    client.indices = MagicMock()
    client.indices.exists.return_value = True

    ensure_settings_index(client)

    client.indices.exists.assert_called_once_with(index=STATUS_INDEX)
    client.indices.create.assert_not_called()


def test_ensure_settings_index_does_not_exist():
    client = MagicMock(spec=Elasticsearch)
    client.indices = MagicMock()
    client.indices.exists.return_value = False

    ensure_settings_index(client)

    client.indices.exists.assert_called_once_with(index=STATUS_INDEX)
    client.indices.create.assert_called_once_with(index=STATUS_INDEX)


def test_get_settings_document_found():
    client = MagicMock(spec=Elasticsearch)
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
    client.get.assert_called_once_with(index=STATUS_INDEX, id=SETTINGS_ID)


def test_get_settings_document_not_found():
    client = MagicMock(spec=Elasticsearch)
    client.get.side_effect = NotFoundError(404, "Not Found", {})

    settings = get_settings(client)

    assert settings is None
    client.get.assert_called_once_with(index=STATUS_INDEX, id=SETTINGS_ID)


@patch("curator.actions.deepfreeze.Elasticsearch")
def test_create_repo_success(mock_es):
    client = mock_es.return_value
    client.snapshot.create_repository.return_value = {"acknowledged": True}

    create_repo(
        client,
        repo_name="test-repo",
        bucket_name="test-bucket",
        base_path="test-path",
        canned_acl="private",
        storage_class="STANDARD",
    )

    client.snapshot.create_repository.assert_called_once_with(
        name="test-repo",
        body={
            "type": "s3",
            "settings": {
                "bucket": "test-bucket",
                "base_path": "test-path",
                "canned_acl": "private",
                "storage_class": "STANDARD",
            },
        },
    )


@patch("curator.actions.deepfreeze.Elasticsearch")
def test_create_repo_dry_run(mock_es):
    client = mock_es.return_value

    create_repo(
        client,
        repo_name="test-repo",
        bucket_name="test-bucket",
        base_path="test-path",
        canned_acl="private",
        storage_class="STANDARD",
        dry_run=True,
    )

    client.snapshot.create_repository.assert_not_called()


@patch("curator.actions.deepfreeze.Elasticsearch")
def test_create_repo_failure(mock_es):
    client = mock_es.return_value
    client.snapshot.create_repository.side_effect = Exception("Some error")

    with pytest.raises(ActionError):
        create_repo(
            client,
            repo_name="test-repo",
            bucket_name="test-bucket",
            base_path="test-path",
            canned_acl="private",
            storage_class="STANDARD",
        )

    client.snapshot.create_repository.assert_called_once_with(
        name="test-repo",
        body={
            "type": "s3",
            "settings": {
                "bucket": "test-bucket",
                "base_path": "test-path",
                "canned_acl": "private",
                "storage_class": "STANDARD",
            },
        },
    )


def test_get_next_suffix_oneup():
    assert get_next_suffix("oneup", "000001", None, None) == "000002"
    assert get_next_suffix("oneup", "000009", None, None) == "000010"
    assert get_next_suffix("oneup", "999999", None, None) == "1000000"


def test_get_next_suffix_date():
    assert get_next_suffix("date", None, 2023, 1) == "2023.01"
    assert get_next_suffix("date", None, 2024, 12) == "2024.12"
    assert get_next_suffix("date", None, 2025, 6) == "2025.06"


def test_get_next_suffix_invalid_style():
    with pytest.raises(ValueError):
        get_next_suffix("invalid_style", "000001", None, None)


def test_get_unmounted_repos_no_repos():
    client = MagicMock(spec=Elasticsearch)
    client.search.return_value = {"hits": {"hits": []}}

    repos = get_unmounted_repos(client)

    assert repos == []
    client.search.assert_called_once_with(
        index=STATUS_INDEX, body={"query": {"match": {"doctype": "repository"}}}
    )


def test_get_unmounted_repos_with_repos():
    client = MagicMock(spec=Elasticsearch)
    client.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "name": "repo1",
                        "bucket": "bucket1",
                        "base_path": "path1",
                        "start": "2023-01-01T00:00:00",
                        "end": "2023-01-02T00:00:00",
                        "is_thawed": False,
                        "is_mounted": False,
                    }
                },
                {
                    "_source": {
                        "name": "repo2",
                        "bucket": "bucket2",
                        "base_path": "path2",
                        "start": "2023-01-03T00:00:00",
                        "end": "2023-01-04T00:00:00",
                        "is_thawed": False,
                        "is_mounted": False,
                    }
                },
            ]
        }
    }

    repos = get_unmounted_repos(client)

    assert len(repos) == 2
    assert repos[0].name == "repo1"
    assert repos[1].name == "repo2"
    client.search.assert_called_once_with(
        index=STATUS_INDEX, body={"query": {"match": {"doctype": "repository"}}}
    )


def test_get_unmounted_repos_with_mounted_repos():
    client = MagicMock(spec=Elasticsearch)
    client.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "name": "repo1",
                        "bucket": "bucket1",
                        "base_path": "path1",
                        "start": "2023-01-01T00:00:00",
                        "end": "2023-01-02T00:00:00",
                        "is_thawed": False,
                        "is_mounted": True,
                    }
                },
                {
                    "_source": {
                        "name": "repo2",
                        "bucket": "bucket2",
                        "base_path": "path2",
                        "start": "2023-01-03T00:00:00",
                        "end": "2023-01-04T00:00:00",
                        "is_thawed": False,
                        "is_mounted": False,
                    }
                },
            ]
        }
    }

    repos = get_unmounted_repos(client)

    assert len(repos) == 2
    assert repos[0].name == "repo1"
    assert repos[1].name == "repo2"
    client.search.assert_called_once_with(
        index=STATUS_INDEX, body={"query": {"match": {"doctype": "repository"}}}
    )


def test_get_repos_no_repos():
    client = MagicMock(spec=Elasticsearch)

    # Ensure 'snapshot' is a mock object before setting return values
    client.snapshot = MagicMock()
    client.snapshot.get_repository.return_value = {}

    repos = get_repos(client, "test-prefix")

    assert repos == []
    client.snapshot.get_repository.assert_called_once()


def test_get_repos_with_matching_repos():
    client = MagicMock(spec=Elasticsearch)
    # Ensure 'snapshot' is a mock object
    client.snapshot = MagicMock()
    client.snapshot.get_repository.return_value = {
        "test-prefix-repo1": {},
        "test-prefix-repo2": {},
        "other-repo": {},
    }

    repos = get_repos(client, "test-prefix")

    assert repos == ["test-prefix-repo1", "test-prefix-repo2"]
    client.snapshot.get_repository.assert_called_once()


def test_get_repos_with_no_matching_repos():
    client = MagicMock(spec=Elasticsearch)
    # Ensure 'snapshot' is a mock object
    client.snapshot = MagicMock()
    client.snapshot.get_repository.return_value = {
        "other-repo1": {},
        "other-repo2": {},
    }

    repos = get_repos(client, "test-prefix")

    assert repos == []
    client.snapshot.get_repository.assert_called_once()


def test_get_repos_with_partial_matching_repos():
    client = MagicMock(spec=Elasticsearch)
    # Ensure 'snapshot' is a mock object
    client.snapshot = MagicMock()
    client.snapshot.get_repository.return_value = {
        "test-prefix-repo1": {},
        "other-repo": {},
        "test-prefix-repo2": {},
    }

    repos = get_repos(client, "test-prefix")

    assert repos == ["test-prefix-repo1", "test-prefix-repo2"]
    client.snapshot.get_repository.assert_called_once()


@patch("curator.actions.deepfreeze.get_all_indices_in_repo")
@patch("curator.actions.deepfreeze.get_timestamp_range")
@patch("curator.actions.deepfreeze.decode_date")
def test_unmount_repo_success(
    mock_decode_date, mock_get_timestamp_range, mock_get_all_indices_in_repo
):
    client = MagicMock(spec=Elasticsearch)
    repo_name = "test-repo"
    repo_info = {
        "settings": {
            "bucket": "test-bucket",
            "base_path": "test-path",
        }
    }
    # Ensure 'snapshot' is a mock object
    client.snapshot = MagicMock()
    mock_get_all_indices_in_repo.return_value = ["index1", "index2"]
    mock_get_timestamp_range.return_value = (datetime(2023, 1, 1), datetime(2023, 1, 2))
    mock_decode_date.side_effect = [datetime(2023, 1, 1), datetime(2023, 1, 2)]
    client.snapshot.get_repository.return_value = {repo_name: repo_info}

    result = unmount_repo(client, repo_name)

    assert isinstance(result, Repository)
    assert result.name == repo_name
    assert result.bucket == "test-bucket"
    assert result.base_path == "test-path"
    assert result.start == datetime(2023, 1, 1)
    assert result.end == datetime(2023, 1, 2)
    assert result.is_mounted is False

    client.snapshot.get_repository.assert_called_once_with(name=repo_name)
    client.index.assert_called_once_with(
        index="deepfreeze-status", document=result.to_dict()
    )
    client.snapshot.delete_repository.assert_called_once_with(name=repo_name)
    client.snapshot.delete_repository.assert_called_once_with(name=repo_name)


@patch("curator.actions.deepfreeze.get_all_indices_in_repo")
@patch("curator.actions.deepfreeze.get_timestamp_range")
@patch("curator.actions.deepfreeze.decode_date")
def test_unmount_repo_not_found(
    mock_decode_date, mock_get_timestamp_range, mock_get_all_indices_in_repo
):
    client = MagicMock(spec=Elasticsearch)
    repo_name = "test-repo"

    # Ensure 'snapshot' is a mock object
    client.snapshot = MagicMock()
    client.snapshot.get_repository.side_effect = NotFoundError(404, "Not Found", {})

    with pytest.raises(NotFoundError):
        unmount_repo(client, repo_name)

    client.snapshot.get_repository.assert_called_once_with(name=repo_name)
    client.index.assert_not_called()
    client.snapshot.delete_repository.assert_not_called()


@patch("curator.actions.deepfreeze.get_all_indices_in_repo")
@patch("curator.actions.deepfreeze.get_timestamp_range")
@patch("curator.actions.deepfreeze.decode_date")
def test_unmount_repo_no_indices(
    mock_decode_date, mock_get_timestamp_range, mock_get_all_indices_in_repo
):
    client = MagicMock(spec=Elasticsearch)
    repo_name = "test-repo"
    repo_info = {
        "settings": {
            "bucket": "test-bucket",
            "base_path": "test-path",
        }
    }
    # Ensure 'snapshot' is a mock object
    client.snapshot = MagicMock()

    mock_get_all_indices_in_repo.return_value = []
    mock_get_timestamp_range.return_value = (datetime(2023, 1, 1), datetime(2023, 1, 2))
    mock_decode_date.side_effect = [datetime(2023, 1, 1), datetime(2023, 1, 2)]
    client.snapshot.get_repository.return_value = {repo_name: repo_info}

    result = unmount_repo(client, repo_name)

    assert isinstance(result, Repository)
    assert result.name == repo_name
    assert result.bucket == "test-bucket"
    assert result.base_path == "test-path"
    assert result.start == datetime(2023, 1, 1)
    assert result.end == datetime(2023, 1, 2)
    assert result.is_mounted is False

    client.snapshot.get_repository.assert_called_once_with(name=repo_name)
    client.index.assert_called_once_with(
        index="deepfreeze-status", document=result.to_dict()
    )
    client.snapshot.delete_repository.assert_called_once_with(name=repo_name)


@patch("curator.actions.deepfreeze.get_all_indices_in_repo")
@patch("curator.actions.deepfreeze.get_timestamp_range")
@patch("curator.actions.deepfreeze.decode_date")
def test_unmount_repo_exception(
    mock_decode_date, mock_get_timestamp_range, mock_get_all_indices_in_repo
):
    client = MagicMock(spec=Elasticsearch)
    repo_name = "test-repo"

    # Ensure 'snapshot' is a mock object
    client.snapshot = MagicMock()
    client.snapshot.get_repository.side_effect = Exception("Some error")

    with pytest.raises(Exception):
        unmount_repo(client, repo_name)

    client.snapshot.get_repository.assert_called_once_with(name=repo_name)
    client.index.assert_not_called()
    client.snapshot.delete_repository.assert_not_called()


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
