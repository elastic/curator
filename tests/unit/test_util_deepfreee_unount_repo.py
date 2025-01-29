""" This module contains tests for the unmount_repo function in the deepfreeze module. """

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

import re
from unittest.mock import Mock

import pytest

from curator.actions.deepfreeze import unmount_repo


@pytest.fixture
def mock_client():
    """Fixture to provide a mock client object."""
    return Mock()


def test_unmount_repo_success(mock_client):
    """Test that unmount_repo successfully deletes a repository."""
    repo = "test-repo"
    status_index = "status-index"

    # Simulate successful repository deletion (we mock the delete_repository method)
    mock_client.snapshot.delete_repository.return_value = {"acknowledged": True}

    # Call the function with the mock client
    unmount_repo(mock_client, repo, status_index)

    # Assert that delete_repository was called with the correct repo name
    mock_client.snapshot.delete_repository.assert_called_once_with(name=repo)


def test_unmount_repo_delete_repository_exception(mock_client):
    """Test that unmount_repo raises an error if deleting the repository fails."""
    repo = "test-repo"
    status_index = "status-index"

    # Simulate a failure when attempting to delete the repository
    mock_client.snapshot.delete_repository.side_effect = Exception(
        "Error deleting repository"
    )

    # Ensure the exception is raised
    with pytest.raises(Exception, match="Error deleting repository"):
        unmount_repo(mock_client, repo, status_index)

    # Check that delete_repository was called with the correct repo name
    mock_client.snapshot.delete_repository.assert_called_once_with(name=repo)
