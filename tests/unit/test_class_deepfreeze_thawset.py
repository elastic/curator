import pytest

from curator.actions.deepfreeze import ThawedRepo, ThawSet


def test_thawed_repo_initialization():
    """Test that a ThawedRepo object is initialized correctly from a dictionary."""
    repo_info = {
        "name": "test-repo",
        "bucket": "test-bucket",
        "base_path": "test/base/path",
    }
    repo = ThawedRepo(repo_info)

    assert repo.repo_name == "test-repo"
    assert repo.bucket_name == "test-bucket"
    assert repo.base_path == "test/base/path"
    assert repo.provider == "aws"  # Default value
    assert repo.indices is None  # Default value if not provided


def test_thawed_repo_with_indices():
    """Test initializing a ThawedRepo with indices."""
    repo_info = {
        "name": "test-repo",
        "bucket": "test-bucket",
        "base_path": "test/base/path",
    }
    indices = ["index1", "index2"]
    repo = ThawedRepo(repo_info, indices)

    assert repo.indices == indices


def test_thawed_repo_add_index():
    """Test that indices can be added to a ThawedRepo."""
    repo_info = {
        "name": "test-repo",
        "bucket": "test-bucket",
        "base_path": "test/base/path",
    }
    repo = ThawedRepo(repo_info, [])

    repo.add_index("index1")
    repo.add_index("index2")

    assert repo.indices == ["index1", "index2"]


def test_thaw_set_add_and_retrieve():
    """Test adding a ThawedRepo to ThawSet and retrieving it."""
    thaw_set = ThawSet()
    repo_info = {
        "name": "test-repo",
        "bucket": "test-bucket",
        "base_path": "test/base/path",
    }
    repo = ThawedRepo(repo_info)

    thaw_set.add(repo)

    assert "test-repo" in thaw_set.thawset  # Key should exist in the dict
    assert (
        thaw_set.thawset["test-repo"] is repo
    )  # Stored object should be the same instance


def test_thaw_set_overwrite():
    """Test that adding a ThawedRepo with the same name overwrites the previous one."""
    thaw_set = ThawSet()
    repo_info1 = {"name": "test-repo", "bucket": "bucket1", "base_path": "path1"}
    repo_info2 = {"name": "test-repo", "bucket": "bucket2", "base_path": "path2"}

    repo1 = ThawedRepo(repo_info1)
    repo2 = ThawedRepo(repo_info2)

    thaw_set.add(repo1)
    thaw_set.add(repo2)

    assert thaw_set.thawset["test-repo"] is repo2  # Latest instance should be stored
    assert (
        thaw_set.thawset["test-repo"].bucket_name == "bucket2"
    )  # Ensure it overwrote correctly


def test_thaw_set_multiple_repos():
    """Test adding multiple repos to ThawSet and retrieving them."""
    thaw_set = ThawSet()
    repo_info1 = {"name": "repo1", "bucket": "bucket1", "base_path": "path1"}
    repo_info2 = {"name": "repo2", "bucket": "bucket2", "base_path": "path2"}

    repo1 = ThawedRepo(repo_info1)
    repo2 = ThawedRepo(repo_info2)

    thaw_set.add(repo1)
    thaw_set.add(repo2)

    assert thaw_set.thawset["repo1"] is repo1
    assert thaw_set.thawset["repo2"] is repo2
    assert len(thaw_set.thawset) == 2  # Ensure correct count of stored repos


def test_thaw_set_no_duplicate_keys():
    """Test that ThawSet behaves like a dictionary and does not allow duplicate keys."""
    thaw_set = ThawSet()
    repo_info1 = {"name": "repo1", "bucket": "bucket1", "base_path": "path1"}
    repo_info2 = {
        "name": "repo1",  # Same name, should replace repo1
        "bucket": "bucket2",
        "base_path": "path2",
    }

    repo1 = ThawedRepo(repo_info1)
    repo2 = ThawedRepo(repo_info2)

    thaw_set.add(repo1)
    thaw_set.add(repo2)

    assert len(thaw_set.thawset) == 1  # Should still be 1 since repo2 replaces repo1
    assert thaw_set.thawset["repo1"] is repo2  # Ensure the replacement worked
    assert (
        thaw_set.thawset["repo1"].bucket_name == "bucket2"
    )  # Ensure new values are stored
