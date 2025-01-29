"""Unit tests for the create_new_bucket function in the deepfreeze module."""

# pylint: disable=missing-function-docstring, redefined-outer-name, pointless-statement, missing-class-docstring, protected-access, attribute-defined-outside-init

import logging
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError
from elasticsearch8.exceptions import NotFoundError

from curator.actions.deepfreeze import create_new_bucket
from curator.exceptions import ActionError


@pytest.fixture
def mock_s3_client():
    """Fixture to provide a mock S3 client."""
    with patch("boto3.client") as mock_boto_client:
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        yield mock_s3


def test_create_new_bucket_success(mock_s3_client):
    """Test successful bucket creation."""
    bucket_name = "test-bucket"

    result = create_new_bucket(bucket_name)

    mock_s3_client.create_bucket.assert_called_once_with(Bucket=bucket_name)
    assert result is None  # Function returns nothing on success


def test_create_new_bucket_dry_run(mock_s3_client):
    """Test dry run mode (should not create a bucket)."""
    bucket_name = "test-bucket"

    result = create_new_bucket(bucket_name, dry_run=True)

    mock_s3_client.create_bucket.assert_not_called()
    assert result is None


def test_create_new_bucket_client_error(mock_s3_client):
    """Test handling of a ClientError when creating a bucket."""
    bucket_name = "test-bucket"
    mock_s3_client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyExists", "Message": "Bucket already exists"}},
        "CreateBucket",
    )

    with pytest.raises(ActionError) as excinfo:
        create_new_bucket(bucket_name)

    mock_s3_client.create_bucket.assert_called_once_with(Bucket=bucket_name)
    assert "BucketAlreadyExists" in str(excinfo.value)


def test_create_new_bucket_unexpected_exception(mock_s3_client):
    """Test handling of unexpected exceptions."""
    bucket_name = "test-bucket"
    mock_s3_client.create_bucket.side_effect = ValueError("Unexpected error")

    with pytest.raises(ValueError, match="Unexpected error"):
        create_new_bucket(bucket_name)

    mock_s3_client.create_bucket.assert_called_once_with(Bucket=bucket_name)
