""" Unit tests for the Rotate class in the deepfreeze action module """

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

from unittest.mock import MagicMock, patch

import pytest

from curator.actions.deepfreeze import RepositoryException, Rotate, Settings


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.indices.exists.return_value = False
    client.snapshot.get_repository.return_value = ["testrepo-000001"]
    client.ilm.get_lifecycle.return_value = {}
    return client


@pytest.fixture
def rotate_instance(mock_client):
    settings_dict = {
        "repo_name_prefix": "testrepo",
        "bucket_name_prefix": "testbucket",
        "base_path_prefix": "testpath",
        "canned_acl": "private",
        "storage_class": "standard",
        "provider": "aws",
        "rotate_by": "path",
        "style": "oneup",
        "last_suffix": "000001",
    }
    settings = Settings(settings_dict)
    with patch('curator.actions.deepfreeze.get_settings', return_value=settings):
        return Rotate(
            client=mock_client,
            keep=6,
            year=2023,
            month=10,
        )


def test_rotate_initialization(rotate_instance):
    assert rotate_instance.settings.repo_name_prefix == "testrepo"
    assert rotate_instance.settings.bucket_name_prefix == "testbucket"
    assert rotate_instance.settings.base_path_prefix == "testpath"
    assert rotate_instance.settings.canned_acl == "private"
    assert rotate_instance.settings.storage_class == "standard"
    assert rotate_instance.settings.provider == "aws"
    assert rotate_instance.settings.rotate_by == "path"
    assert rotate_instance.settings.style == "oneup"
    assert rotate_instance.new_repo_name == "testrepo-000002"
    assert rotate_instance.new_bucket_name == "testbucket"
    assert rotate_instance.base_path == "testpath-000002"


def test_rotate_do_dry_run(rotate_instance, mock_client):
    with patch(
        'curator.actions.deepfreeze.create_new_bucket'
    ) as mock_create_bucket, patch(
        'curator.actions.deepfreeze.create_new_repo'
    ) as mock_create_repo, patch(
        'curator.actions.deepfreeze.Rotate.update_ilm_policies'
    ) as mock_update_ilm, patch(
        'curator.actions.deepfreeze.Rotate.unmount_oldest_repos'
    ) as mock_unmount_repos:
        rotate_instance.do_dry_run()
        mock_create_bucket.assert_called_once_with("testbucket", dry_run=True)
        mock_create_repo.assert_called_once_with(
            mock_client,
            "testrepo-000002",
            "testbucket",
            "testpath-000002",
            "private",
            "standard",
            dry_run=True,
        )
        mock_update_ilm.assert_called_once_with(dry_run=True)
        mock_unmount_repos.assert_called_once_with(dry_run=True)


def test_rotate_do_action(rotate_instance, mock_client):
    with patch(
        'curator.actions.deepfreeze.create_new_bucket'
    ) as mock_create_bucket, patch(
        'curator.actions.deepfreeze.create_new_repo'
    ) as mock_create_repo, patch(
        'curator.actions.deepfreeze.Rotate.update_ilm_policies'
    ) as mock_update_ilm, patch(
        'curator.actions.deepfreeze.Rotate.unmount_oldest_repos'
    ) as mock_unmount_repos, patch(
        'curator.actions.deepfreeze.ensure_settings_index'
    ) as mock_ensure_index, patch(
        'curator.actions.deepfreeze.save_settings'
    ) as mock_save_settings:
        rotate_instance.do_action()
        mock_ensure_index.assert_called_once_with(mock_client)
        mock_save_settings.assert_called_once_with(
            mock_client, rotate_instance.settings
        )
        mock_create_bucket.assert_called_once_with("testbucket")
        mock_create_repo.assert_called_once_with(
            mock_client,
            "testrepo-000002",
            "testbucket",
            "testpath-000002",
            "private",
            "standard",
        )
        mock_update_ilm.assert_called_once()
        mock_unmount_repos.assert_called_once()
