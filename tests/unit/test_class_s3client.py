from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from curator.exceptions import ActionError
from curator.s3client import AwsS3Client, s3_client_factory


def test_s3_client_factory_aws():
    client = s3_client_factory("aws")
    assert isinstance(client, AwsS3Client)


def test_s3_client_factory_invalid_provider():
    with pytest.raises(ValueError):
        s3_client_factory("invalid_provider")


def test_s3_client_factory_not_implemented_provider():
    with pytest.raises(NotImplementedError):
        s3_client_factory("gcp")


@patch("boto3.client")
def test_aws_s3_client_create_bucket_success(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    client = AwsS3Client()
    client.create_bucket("test-bucket")
    mock_s3.create_bucket.assert_called_once_with(Bucket="test-bucket")


@patch("boto3.client")
def test_aws_s3_client_create_bucket_failure(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.create_bucket.side_effect = ClientError(
        {
            "Error": {
                "Code": "BucketAlreadyExists",
                "Message": "The requested bucket name is not available.",
            }
        },
        "CreateBucket",
    )
    client = AwsS3Client()
    with pytest.raises(ActionError):
        client.create_bucket("test-bucket")
