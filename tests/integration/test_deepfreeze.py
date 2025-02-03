from unittest.mock import MagicMock, patch

import pytest

from curator.actions.deepfreeze import Rotate, get_next_suffix, get_repos


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def rotate_instance(mock_client):
    with patch("curator.actions.deepfreeze.get_settings") as mock_get_settings:
        mock_get_settings.return_value = MagicMock(
            repo_name_prefix="deepfreeze",
            bucket_name_prefix="deepfreeze",
            base_path_prefix="snapshots",
            canned_acl="private",
            storage_class="intelligent_tiering",
            provider="aws",
            rotate_by="path",
            style="oneup",
            last_suffix="000001",
        )
        return Rotate(mock_client)


def test_rotate_init(rotate_instance):
    assert rotate_instance.settings.repo_name_prefix == "deepfreeze"
    assert rotate_instance.settings.bucket_name_prefix == "deepfreeze"
    assert rotate_instance.settings.base_path_prefix == "snapshots"
    assert rotate_instance.settings.canned_acl == "private"
    assert rotate_instance.settings.storage_class == "intelligent_tiering"
    assert rotate_instance.settings.provider == "aws"
    assert rotate_instance.settings.rotate_by == "path"
    assert rotate_instance.settings.style == "oneup"
    assert rotate_instance.settings.last_suffix == "000001"


def test_rotate_do_dry_run(rotate_instance):
    with (
        patch.object(
            rotate_instance, "update_ilm_policies"
        ) as mock_update_ilm_policies,
        patch.object(
            rotate_instance, "unmount_oldest_repos"
        ) as mock_unmount_oldest_repos,
        patch("curator.actions.deepfreeze.create_new_repo") as mock_create_new_repo,
    ):
        rotate_instance.do_dry_run()
        mock_create_new_repo.assert_called_once_with(
            rotate_instance.client,
            rotate_instance.new_repo_name,
            rotate_instance.new_bucket_name,
            rotate_instance.base_path,
            rotate_instance.settings.canned_acl,
            rotate_instance.settings.storage_class,
            dry_run=True,
        )
        mock_update_ilm_policies.assert_called_once_with(dry_run=True)
        mock_unmount_oldest_repos.assert_called_once_with(dry_run=True)


def test_rotate_do_action(rotate_instance):
    with (
        patch(
            "curator.actions.deepfreeze.ensure_settings_index"
        ) as mock_ensure_settings_index,
        patch("curator.actions.deepfreeze.save_settings") as mock_save_settings,
        patch("curator.actions.deepfreeze.create_new_repo") as mock_create_new_repo,
        patch.object(
            rotate_instance, "update_ilm_policies"
        ) as mock_update_ilm_policies,
        patch.object(
            rotate_instance, "unmount_oldest_repos"
        ) as mock_unmount_oldest_repos,
    ):
        rotate_instance.do_action()
        mock_ensure_settings_index.assert_called_once_with(rotate_instance.client)
        mock_save_settings.assert_called_once_with(
            rotate_instance.client, rotate_instance.settings
        )
        mock_create_new_repo.assert_called_once_with(
            rotate_instance.client,
            rotate_instance.new_repo_name,
            rotate_instance.new_bucket_name,
            rotate_instance.base_path,
            rotate_instance.settings.canned_acl,
            rotate_instance.settings.storage_class,
        )
        mock_update_ilm_policies.assert_called_once()
        mock_unmount_oldest_repos.assert_called_once()


def test_rotate_get_next_suffix():
    assert get_next_suffix("oneup", "000001", None, None) == "000002"
    assert get_next_suffix("date", None, 2023, 10) == "2023.10"
    with pytest.raises(ValueError):
        get_next_suffix("invalid_style", None, None, None)


def test_rotate_get_repos(mock_client):
    mock_client.snapshot.get_repository.return_value = {
        "deepfreeze-000001": {},
        "deepfreeze-000002": {},
        "other-repo": {},
    }
    repos = get_repos(mock_client, "deepfreeze")
    assert repos == ["deepfreeze-000001", "deepfreeze-000002"]


def test_rotate_unmount_oldest_repos(rotate_instance):
    rotate_instance.repo_list = [
        "deepfreeze-000001",
        "deepfreeze-000002",
        "deepfreeze-000003",
    ]
    rotate_instance.keep = 2
    with patch("curator.actions.deepfreeze.unmount_repo") as mock_unmount_repo:
        rotate_instance.unmount_oldest_repos()
        mock_unmount_repo.assert_called_once_with(
            rotate_instance.client, "deepfreeze-000001"
        )
