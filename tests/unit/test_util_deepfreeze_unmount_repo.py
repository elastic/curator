"""This module contains tests for the unmount_repo function in the deepfreeze module."""

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

from unittest.mock import Mock

import pytest

from curator.actions.deepfreeze import STATUS_INDEX, Repository, unmount_repo


@pytest.fixture
def mock_client():
    """Fixture to provide a mock client object."""
    return Mock()


def test_unmount_repo_success(mock_client):
    """Test that unmount_repo successfully records repo details and deletes the repository."""
    repo = "test-repo"

    # Mock repository info response
    mock_client.snapshot.get_repository.return_value = {
        "settings": {
            "bucket": "test-bucket",
            "base_path": "test/base/path",
        }
    }

    # Call the function with the mock client
    unmount_repo(mock_client, repo)

    # Ensure get_repository was called with the correct repo name
    mock_client.snapshot.get_repository.assert_called_once_with(name=repo)

    # Ensure the create method was called with the correct repository document
    expected_repodoc = Repository(
        {
            "name": repo,
            "bucket": "test-bucket",
            "base_path": "test/base/path",
            "is_mounted": False,
            "start": None,
            "end": None,
        }
    )
    mock_client.create.assert_called_once_with(
        index=STATUS_INDEX, document=expected_repodoc
    )

    # Ensure delete_repository was called to remove the repo
    mock_client.snapshot.delete_repository.assert_called_once_with(name=repo)


def test_unmount_repo_get_repository_exception(mock_client):
    """Test that an exception during get_repository raises an error."""
    repo = "test-repo"

    # Simulate an exception when fetching repository details
    mock_client.snapshot.get_repository.side_effect = Exception(
        "Error fetching repository info"
    )

    with pytest.raises(Exception, match="Error fetching repository info"):
        unmount_repo(mock_client, repo)

    # Ensure delete_repository was not called since an error occurred earlier
    mock_client.snapshot.delete_repository.assert_not_called()
    mock_client.create.assert_not_called()


def test_unmount_repo_create_exception(mock_client):
    """Test that an exception during create() raises an error and stops execution."""
    repo = "test-repo"

    # Mock repository info response
    mock_client.snapshot.get_repository.return_value = {
        "settings": {
            "bucket": "test-bucket",
            "base_path": "test/base/path",
        }
    }

    # Simulate an exception when creating the repository record
    mock_client.create.side_effect = Exception("Error creating repository record")

    with pytest.raises(Exception, match="Error creating repository record"):
        unmount_repo(mock_client, repo)

    # Ensure delete_repository was not called since an error occurred earlier
    mock_client.snapshot.delete_repository.assert_not_called()


def test_unmount_repo_delete_repository_exception(mock_client):
    """Test that an exception during delete_repository is raised."""
    repo = "test-repo"

    # Mock repository info response
    mock_client.snapshot.get_repository.return_value = {
        "settings": {
            "bucket": "test-bucket",
            "base_path": "test/base/path",
        }
    }

    # Simulate an exception when deleting the repository
    mock_client.snapshot.delete_repository.side_effect = Exception(
        "Error deleting repository"
    )

    with pytest.raises(Exception, match="Error deleting repository"):
        unmount_repo(mock_client, repo)

    # Ensure get_repository and create were called before failure
    mock_client.snapshot.get_repository.assert_called_once_with(name=repo)
    mock_client.create.assert_called_once()
