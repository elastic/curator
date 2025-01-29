""" This module contains unit tests for the get_repos function in the deepfreeze module. """

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

import re
from unittest.mock import Mock

import pytest

from curator.actions.deepfreeze import get_repos
from curator.exceptions import ActionError


@pytest.fixture
def mock_client():
    """Fixture to provide a mock client object."""
    return Mock()


def test_get_repos_success(mock_client):
    """Test that get_repos returns repositories matching the prefix."""
    repo_name_prefix = "test"

    # Simulate client.get_repository returning a list of repositories
    mock_client.snapshot.get_repository.return_value = [
        "test-repo-1",
        "test-repo-2",
        "prod-repo",
        "test-repo-3",
    ]

    # Call the function with the mock client
    result = get_repos(mock_client, repo_name_prefix)

    # Check that the function only returns repos that start with "test"
    assert result == ["test-repo-1", "test-repo-2", "test-repo-3"]


def test_get_repos_no_match(mock_client):
    """Test that get_repos returns an empty list when no repos match the prefix."""
    repo_name_prefix = "prod"

    # Simulate client.get_repository returning a list of repositories
    mock_client.snapshot.get_repository.return_value = [
        "test-repo-1",
        "test-repo-2",
        "test-repo-3",
    ]

    # Call the function with the mock client
    result = get_repos(mock_client, repo_name_prefix)

    # Check that the result is empty as no repos start with "prod"
    assert result == []


def test_get_repos_regex_pattern(mock_client):
    """Test that get_repos correctly matches repos based on the regex prefix."""
    repo_name_prefix = "test.*-2$"  # Match repos ending with "-2"

    # Simulate client.get_repository returning a list of repositories
    mock_client.snapshot.get_repository.return_value = [
        "test-repo-1",
        "test-repo-2",
        "prod-repo",
        "test-repo-3",
    ]

    # Call the function with the mock client
    result = get_repos(mock_client, repo_name_prefix)

    # Check that the regex correctly matches "test-repo-2"
    assert result == ["test-repo-2"]


def test_get_repos_empty_list(mock_client):
    """Test that get_repos returns an empty list if no repositories are returned."""
    repo_name_prefix = "test"

    # Simulate client.get_repository returning an empty list
    mock_client.snapshot.get_repository.return_value = []

    # Call the function with the mock client
    result = get_repos(mock_client, repo_name_prefix)

    # Check that the result is an empty list as no repos are returned
    assert result == []
