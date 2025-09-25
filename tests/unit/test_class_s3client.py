from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from curator.s3client import AwsS3Client, S3Client, s3_client_factory
from tests.integration import random_suffix


def test_create_bucket():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.client.bucket_exists.return_value = False

    assert s3.client.bucket_exists("test-bucket") is False

    # FIXME: This test is not working as expected. Something in the way it's mocked up
    # FIXME: means that the call to create_bucket gets a different result when
    # FIXME: bucket_exists() is called.
    s3.create_bucket("test-bucket")
    s3.client.create_bucket.assert_called_with(Bucket="test-bucket")


def test_create_bucket_error():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "Error"}}, "create_bucket"
    )

    with pytest.raises(Exception):
        s3.create_bucket("test-bucket")


def test_thaw():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.client.head_object.return_value = {"StorageClass": "GLACIER"}

    s3.thaw(
        "test-bucket",
        "base_path",
        ["base_path/file1", "base_path/file2"],
        7,
        "Standard",
    )
    assert s3.client.restore_object.call_count == 2


def test_thaw_skip_non_glacier():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.client.head_object.return_value = {"StorageClass": "STANDARD"}

    s3.thaw("test-bucket", "base_path", ["base_path/file1"], 7, "Standard")
    s3.client.restore_object.assert_not_called()


def test_refreeze():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "base_path/file1"}]}
    ]

    s3.refreeze("test-bucket", "base_path", "GLACIER")
    s3.client.copy_object.assert_called_with(
        Bucket="test-bucket",
        CopySource={"Bucket": "test-bucket", "Key": "base_path/file1"},
        Key="base_path/file1",
        StorageClass="GLACIER",
        MetadataDirective="COPY",
    )


def test_s3_client_factory():
    assert isinstance(s3_client_factory("aws"), AwsS3Client)
    with pytest.raises(NotImplementedError):
        s3_client_factory("gcp")
    with pytest.raises(NotImplementedError):
        s3_client_factory("azure")
    with pytest.raises(ValueError):
        s3_client_factory("unknown")


def test_s3_client_init():
    with patch("boto3.client") as mock_boto:
        s3 = AwsS3Client()
        mock_boto.assert_called_with("s3")


def test_thaw_invalid_key():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.client.head_object.return_value = {"StorageClass": "GLACIER"}

    s3.thaw("test-bucket", "base_path", ["wrong_path/file1"], 7, "Standard")
    s3.client.restore_object.assert_not_called()


def test_refreeze_no_contents():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.client.get_paginator.return_value.paginate.return_value = [{}]

    s3.refreeze("test-bucket", "base_path", "GLACIER")
    s3.client.copy_object.assert_not_called()


def test_uniimplemented():
    s3 = S3Client()
    with pytest.raises(NotImplementedError):
        s3.create_bucket("test-bucket")
    with pytest.raises(NotImplementedError):
        s3.thaw("test-bucket", "base_path", ["base_path/file1"], 7, "Standard")
    with pytest.raises(NotImplementedError):
        s3.refreeze("test-bucket", "base_path", "GLACIER")
