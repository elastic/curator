""" Unit tests for the deepfreeze setup action """

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init
from unittest.mock import MagicMock, patch

import pytest

from curator.actions.deepfreeze import (
    STATUS_INDEX,
    RepositoryException,
    Settings,
    Setup,
)


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.indices.exists.return_value = False
    client.snapshot.get_repository.return_value = {}
    return client


@pytest.fixture
def setup_instance(mock_client):
    return Setup(
        client=mock_client,
        year=2023,
        month=10,
        repo_name_prefix="testrepo",
        bucket_name_prefix="testbucket",
        base_path_prefix="testpath",
        canned_acl="private",
        storage_class="standard",
        provider="aws",
        rotate_by="path",
        style="oneup",
    )


def test_setup_initialization(setup_instance):
    assert setup_instance.settings.repo_name_prefix == "testrepo"
    assert setup_instance.settings.bucket_name_prefix == "testbucket"
    assert setup_instance.settings.base_path_prefix == "testpath"
    assert setup_instance.settings.canned_acl == "private"
    assert setup_instance.settings.storage_class == "standard"
    assert setup_instance.settings.provider == "aws"
    assert setup_instance.settings.rotate_by == "path"
    assert setup_instance.settings.style == "oneup"
    assert setup_instance.new_repo_name == "testrepo-000001"
    assert setup_instance.new_bucket_name == "testbucket"
    assert setup_instance.base_path == "testpath-000001"


def test_setup_do_dry_run(setup_instance, mock_client):
    with patch(
        'curator.actions.deepfreeze.create_new_bucket'
    ) as mock_create_bucket, patch(
        'curator.actions.deepfreeze.create_new_repo'
    ) as mock_create_repo:
        setup_instance.do_dry_run()
        mock_create_bucket.assert_called_once_with("testbucket", dry_run=True)
        mock_create_repo.assert_called_once_with(
            mock_client,
            "testrepo-000001",
            "testbucket",
            "testpath-000001",
            "private",
            "standard",
            dry_run=True,
        )


def test_setup_do_action(setup_instance, mock_client):
    with patch(
        'curator.actions.deepfreeze.create_new_bucket'
    ) as mock_create_bucket, patch(
        'curator.actions.deepfreeze.create_new_repo'
    ) as mock_create_repo, patch(
        'curator.actions.deepfreeze.ensure_settings_index'
    ) as mock_ensure_index, patch(
        'curator.actions.deepfreeze.save_settings'
    ) as mock_save_settings:
        setup_instance.do_action()
        mock_ensure_index.assert_called_once_with(mock_client)
        mock_save_settings.assert_called_once_with(mock_client, setup_instance.settings)
        mock_create_bucket.assert_called_once_with("testbucket")
        mock_create_repo.assert_called_once_with(
            mock_client,
            "testrepo-000001",
            "testbucket",
            "testpath-000001",
            "private",
            "standard",
        )


def test_setup_existing_repo_exception(mock_client):
    mock_client.snapshot.get_repository.return_value = {"testrepo-000001": {}}
    with pytest.raises(RepositoryException):
        Setup(
            client=mock_client,
            year=2023,
            month=10,
            repo_name_prefix="testrepo",
            bucket_name_prefix="testbucket",
            base_path_prefix="testpath",
            canned_acl="private",
            storage_class="standard",
            provider="aws",
            rotate_by="path",
            style="oneup",
        )
