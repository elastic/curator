"""Test the deepfreee Repository class"""

# pylint: disable=missing-function-docstring, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init
from curator.actions.deepfreeze import ThawedRepo, ThawSet


def test_thawed_repo_initialization():
    """Test that a ThawedRepo object is initialized correctly."""
    repo_name = "test-repo"
    repo = ThawedRepo(repo_name)

    assert repo.repo_name == repo_name
    assert repo.bucket_name == ""  # Default value
    assert repo.base_path == ""  # Default value
    assert repo.provider == "aws"  # Default value
    assert (
        isinstance(repo.indices, list) and len(repo.indices) == 0
    )  # Empty list by default


def test_thaw_set_add_and_retrieve():
    """Test adding a ThawedRepo to ThawSet and retrieving it."""
    thaw_set = ThawSet()
    repo = ThawedRepo("test-repo")

    thaw_set.add(repo)

    assert "test-repo" in thaw_set  # Key should exist in the dict
    assert thaw_set["test-repo"] is repo  # Stored object should be the same instance


def test_thaw_set_overwrite():
    """Test that adding a ThawedRepo with the same name overwrites the previous one."""
    thaw_set = ThawSet()
    repo1 = ThawedRepo("test-repo")
    repo2 = ThawedRepo("test-repo")  # New instance with same name

    thaw_set.add(repo1)
    thaw_set.add(repo2)

    assert thaw_set["test-repo"] is repo2  # Latest instance should be stored


def test_thaw_set_multiple_repos():
    """Test adding multiple repos to ThawSet and retrieving them."""
    thaw_set = ThawSet()
    repo1 = ThawedRepo("repo1")
    repo2 = ThawedRepo("repo2")

    thaw_set.add(repo1)
    thaw_set.add(repo2)

    assert thaw_set["repo1"] is repo1
    assert thaw_set["repo2"] is repo2
    assert len(thaw_set) == 2  # Ensure correct count of stored repos


def test_thaw_set_no_duplicate_keys():
    """Test that ThawSet behaves like a dictionary and does not allow duplicate keys."""
    thaw_set = ThawSet()
    repo1 = ThawedRepo("repo1")
    repo2 = ThawedRepo("repo1")  # Same name, should replace repo1

    thaw_set.add(repo1)
    thaw_set.add(repo2)

    assert len(thaw_set) == 1  # Should still be 1 since repo2 replaces repo1
    assert thaw_set["repo1"] is repo2  # Ensure the replacement worked
