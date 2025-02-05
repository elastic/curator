"""This module contains tests for the unmount_repo function in the deepfreeze module."""

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

from unittest.mock import MagicMock

import pytest

from curator.actions.deepfreeze import STATUS_INDEX, Repository, unmount_repo


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.snapshot.get_repository.return_value = {
        "settings": {"bucket": "test-bucket", "base_path": "test-path"}
    }
    return client


def test_unmount_repo(mock_client, mocker):
    # Mock dependencies using mocker
    mock_get_timestamp_range = mocker.patch(
        "curator.actions.deepfreeze.get_timestamp_range",
        return_value=("2024-01-01", "2024-01-31"),
    )
    mock_get_snapshot_indices = mocker.patch(
        "curator.actions.deepfreeze.get_snapshot_indices",
        return_value=["index1", "index2"],
    )
    mock_repository = mocker.patch("curator.actions.deepfreeze.Repository")
    mock_logging = mocker.patch(
        "curator.actions.deepfreeze.logging.getLogger", return_value=MagicMock()
    )

    unmount_repo(mock_client, "test-repo")

    # Assertions
    mock_client.snapshot.get_repository.assert_called_once_with(name="test-repo")
    mock_get_snapshot_indices.assert_called_once_with(mock_client, "test-repo")
    mock_get_timestamp_range.assert_called_once_with(mock_client, ["index1", "index2"])
    mock_repository.assert_called_once()
    mock_client.create.assert_called_once()
    mock_client.snapshot.delete_repository.assert_called_once_with(name="test-repo")
