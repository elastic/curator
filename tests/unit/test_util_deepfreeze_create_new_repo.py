"""This module contains unit tests for the create_new_repo function in the deepfreeze module."""

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

from unittest.mock import Mock

import pytest

from curator.actions.deepfreeze import create_repo
from curator.exceptions import ActionError


@pytest.fixture
def mock_client():
    """Fixture to provide a mock client object."""
    return Mock()


def test_create_new_repo_success(mock_client):
    """Test for successful repository creation."""
    repo_name = "test-repo"
    bucket_name = "test-bucket"
    base_path = "test/base/path"
    canned_acl = "private"
    storage_class = "STANDARD"

    # Simulate a successful response from the client's create_repository method
    mock_client.snapshot.create_repository.return_value = {"acknowledged": True}

    create_repo(
        mock_client, repo_name, bucket_name, base_path, canned_acl, storage_class
    )

    # Assert that create_repository was called with the correct parameters
    mock_client.snapshot.create_repository.assert_called_once_with(
        name=repo_name,
        body={
            "type": "s3",
            "settings": {
                "bucket": bucket_name,
                "base_path": base_path,
                "canned_acl": canned_acl,
                "storage_class": storage_class,
            },
        },
    )


def test_create_new_repo_dry_run(mock_client):
    """Test for dry run (repository should not be created)."""
    repo_name = "test-repo"
    bucket_name = "test-bucket"
    base_path = "test/base/path"
    canned_acl = "private"
    storage_class = "STANDARD"

    create_repo(
        mock_client,
        repo_name,
        bucket_name,
        base_path,
        canned_acl,
        storage_class,
        dry_run=True,
    )

    # Ensure that the repository creation method was not called during dry run
    mock_client.snapshot.create_repository.assert_not_called()


def test_create_new_repo_exception(mock_client):
    """Test that an exception during repository creation raises an ActionError."""
    repo_name = "test-repo"
    bucket_name = "test-bucket"
    base_path = "test/base/path"
    canned_acl = "private"
    storage_class = "STANDARD"

    # Simulate an exception being thrown by the create_repository method
    mock_client.snapshot.create_repository.side_effect = Exception(
        "Error creating repo"
    )

    with pytest.raises(ActionError, match="Error creating repo"):
        create_repo(
            mock_client, repo_name, bucket_name, base_path, canned_acl, storage_class
        )

    # Ensure that the exception was caught and raised as ActionError
    mock_client.snapshot.create_repository.assert_called_once_with(
        name=repo_name,
        body={
            "type": "s3",
            "settings": {
                "bucket": bucket_name,
                "base_path": base_path,
                "canned_acl": canned_acl,
                "storage_class": storage_class,
            },
        },
    )
