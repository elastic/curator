"""Test S3Client classes"""
from unittest.mock import MagicMock, patch, call
import pytest
from botocore.exceptions import ClientError
from curator.exceptions import ActionError
from curator.s3client import AwsS3Client, S3Client, s3_client_factory


class TestS3ClientAbstract:
    """Test abstract S3Client class"""

    def test_abstract_methods_not_implemented(self):
        """Test that abstract methods raise NotImplementedError"""
        # S3Client is abstract, cannot instantiate directly
        with pytest.raises(TypeError):
            S3Client()


class TestAwsS3Client:
    """Test AwsS3Client class"""

    def setup_method(self):
        """Setup for each test"""
        with patch('boto3.client'):
            self.s3 = AwsS3Client()
            self.s3.client = MagicMock()

    def test_init(self):
        """Test AwsS3Client initialization"""
        with patch('boto3.client') as mock_boto:
            s3 = AwsS3Client()
            mock_boto.assert_called_with("s3")
            assert s3.loggit is not None

    def test_create_bucket_success(self):
        """Test successful bucket creation"""
        self.s3.bucket_exists = MagicMock(return_value=False)
        self.s3.create_bucket("test-bucket")
        self.s3.client.create_bucket.assert_called_with(Bucket="test-bucket")

    def test_create_bucket_already_exists(self):
        """Test bucket creation when bucket already exists"""
        self.s3.bucket_exists = MagicMock(return_value=True)
        with pytest.raises(ActionError, match="already exists"):
            self.s3.create_bucket("test-bucket")

    def test_create_bucket_client_error(self):
        """Test bucket creation with ClientError"""
        self.s3.bucket_exists = MagicMock(return_value=False)
        self.s3.client.create_bucket.side_effect = ClientError(
            {"Error": {"Code": "BucketAlreadyExists"}}, "create_bucket"
        )
        with pytest.raises(ActionError):
            self.s3.create_bucket("test-bucket")

    def test_bucket_exists_true(self):
        """Test bucket_exists returns True when bucket exists"""
        self.s3.client.head_bucket.return_value = {}
        assert self.s3.bucket_exists("test-bucket") is True
        self.s3.client.head_bucket.assert_called_with(Bucket="test-bucket")

    def test_bucket_exists_false(self):
        """Test bucket_exists returns False when bucket doesn't exist"""
        self.s3.client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_bucket"
        )
        assert self.s3.bucket_exists("test-bucket") is False

    def test_bucket_exists_other_error(self):
        """Test bucket_exists raises ActionError for non-404 errors"""
        self.s3.client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "403"}}, "head_bucket"
        )
        with pytest.raises(ActionError):
            self.s3.bucket_exists("test-bucket")

    def test_thaw_glacier_objects(self):
        """Test thawing objects from Glacier"""
        self.s3.client.head_object.return_value = {"StorageClass": "GLACIER"}

        self.s3.thaw(
            "test-bucket",
            "base_path",
            ["base_path/file1", "base_path/file2"],
            7,
            "Standard"
        )

        assert self.s3.client.restore_object.call_count == 2
        self.s3.client.restore_object.assert_any_call(
            Bucket="test-bucket",
            Key="base_path/file1",
            RestoreRequest={
                "Days": 7,
                "GlacierJobParameters": {"Tier": "Standard"}
            }
        )

    def test_thaw_deep_archive_objects(self):
        """Test thawing objects from Deep Archive"""
        self.s3.client.head_object.return_value = {"StorageClass": "DEEP_ARCHIVE"}

        self.s3.thaw(
            "test-bucket",
            "base_path",
            ["base_path/file1"],
            7,
            "Expedited"
        )

        self.s3.client.restore_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="base_path/file1",
            RestoreRequest={
                "Days": 7,
                "GlacierJobParameters": {"Tier": "Expedited"}
            }
        )

    def test_thaw_skip_non_glacier(self):
        """Test thaw skips non-Glacier storage classes"""
        self.s3.client.head_object.return_value = {"StorageClass": "STANDARD"}

        self.s3.thaw("test-bucket", "base_path", ["base_path/file1"], 7, "Standard")
        self.s3.client.restore_object.assert_not_called()

    def test_thaw_skip_wrong_path(self):
        """Test thaw skips objects outside base_path"""
        self.s3.client.head_object.return_value = {"StorageClass": "GLACIER"}

        self.s3.thaw("test-bucket", "base_path", ["wrong_path/file1"], 7, "Standard")
        self.s3.client.restore_object.assert_not_called()

    def test_thaw_exception_handling(self):
        """Test thaw handles exceptions gracefully"""
        self.s3.client.head_object.side_effect = Exception("Test error")

        # Should not raise, just log the error
        self.s3.thaw("test-bucket", "base_path", ["base_path/file1"], 7, "Standard")
        self.s3.client.restore_object.assert_not_called()

    def test_refreeze_success(self):
        """Test successful refreezing of objects"""
        self.s3.client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [
                {"Key": "base_path/file1"},
                {"Key": "base_path/file2"}
            ]}
        ]

        self.s3.refreeze("test-bucket", "base_path", "GLACIER")

        assert self.s3.client.copy_object.call_count == 2
        self.s3.client.copy_object.assert_any_call(
            Bucket="test-bucket",
            CopySource={"Bucket": "test-bucket", "Key": "base_path/file1"},
            Key="base_path/file1",
            StorageClass="GLACIER"
        )

    def test_refreeze_deep_archive(self):
        """Test refreezing to Deep Archive"""
        self.s3.client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "base_path/file1"}]}
        ]

        self.s3.refreeze("test-bucket", "base_path", "DEEP_ARCHIVE")

        self.s3.client.copy_object.assert_called_with(
            Bucket="test-bucket",
            CopySource={"Bucket": "test-bucket", "Key": "base_path/file1"},
            Key="base_path/file1",
            StorageClass="DEEP_ARCHIVE"
        )

    def test_refreeze_no_contents(self):
        """Test refreeze when no contents returned"""
        self.s3.client.get_paginator.return_value.paginate.return_value = [{}]

        self.s3.refreeze("test-bucket", "base_path", "GLACIER")
        self.s3.client.copy_object.assert_not_called()

    def test_refreeze_exception_handling(self):
        """Test refreeze handles exceptions gracefully"""
        self.s3.client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "base_path/file1"}]}
        ]
        self.s3.client.copy_object.side_effect = Exception("Test error")

        # Should not raise, just log the error
        self.s3.refreeze("test-bucket", "base_path", "GLACIER")

    def test_list_objects_success(self):
        """Test successful listing of objects"""
        mock_objects = [
            {"Key": "file1", "Size": 100},
            {"Key": "file2", "Size": 200}
        ]
        self.s3.client.get_paginator.return_value.paginate.return_value = [
            {"Contents": mock_objects}
        ]

        result = self.s3.list_objects("test-bucket", "prefix")

        assert result == mock_objects
        self.s3.client.get_paginator.assert_called_with("list_objects_v2")

    def test_list_objects_multiple_pages(self):
        """Test listing objects across multiple pages"""
        self.s3.client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "file1"}]},
            {"Contents": [{"Key": "file2"}]}
        ]

        result = self.s3.list_objects("test-bucket", "prefix")

        assert len(result) == 2
        assert result[0]["Key"] == "file1"
        assert result[1]["Key"] == "file2"

    def test_list_objects_no_contents(self):
        """Test listing objects when no contents"""
        self.s3.client.get_paginator.return_value.paginate.return_value = [{}]

        result = self.s3.list_objects("test-bucket", "prefix")
        assert result == []

    def test_delete_bucket_success(self):
        """Test successful bucket deletion"""
        self.s3.delete_bucket("test-bucket")
        self.s3.client.delete_bucket.assert_called_with(Bucket="test-bucket")

    def test_delete_bucket_error(self):
        """Test bucket deletion error"""
        self.s3.client.delete_bucket.side_effect = ClientError(
            {"Error": {"Code": "BucketNotEmpty"}}, "delete_bucket"
        )

        with pytest.raises(ActionError):
            self.s3.delete_bucket("test-bucket")

    def test_put_object_success(self):
        """Test successful object put"""
        self.s3.put_object("test-bucket", "key", "body content")
        self.s3.client.put_object.assert_called_with(
            Bucket="test-bucket",
            Key="key",
            Body="body content"
        )

    def test_put_object_empty_body(self):
        """Test putting object with empty body"""
        self.s3.put_object("test-bucket", "key")
        self.s3.client.put_object.assert_called_with(
            Bucket="test-bucket",
            Key="key",
            Body=""
        )

    def test_put_object_error(self):
        """Test put object error"""
        self.s3.client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "put_object"
        )

        with pytest.raises(ActionError):
            self.s3.put_object("test-bucket", "key", "body")

    def test_list_buckets_success(self):
        """Test successful bucket listing"""
        self.s3.client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bucket1"},
                {"Name": "bucket2"},
                {"Name": "test-bucket3"}
            ]
        }

        result = self.s3.list_buckets()
        assert result == ["bucket1", "bucket2", "test-bucket3"]

    def test_list_buckets_with_prefix(self):
        """Test bucket listing with prefix filter"""
        self.s3.client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bucket1"},
                {"Name": "test-bucket2"},
                {"Name": "test-bucket3"}
            ]
        }

        result = self.s3.list_buckets(prefix="test-")
        assert result == ["test-bucket2", "test-bucket3"]

    def test_list_buckets_empty(self):
        """Test listing buckets when none exist"""
        self.s3.client.list_buckets.return_value = {"Buckets": []}

        result = self.s3.list_buckets()
        assert result == []

    def test_list_buckets_error(self):
        """Test bucket listing error"""
        self.s3.client.list_buckets.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "list_buckets"
        )

        with pytest.raises(ActionError):
            self.s3.list_buckets()

    def test_copy_object_success(self):
        """Test successful object copy"""
        self.s3.copy_object(
            Bucket="dest-bucket",
            Key="dest-key",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"},
            StorageClass="STANDARD_IA"
        )

        self.s3.client.copy_object.assert_called_with(
            Bucket="dest-bucket",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"},
            Key="dest-key",
            StorageClass="STANDARD_IA"
        )

    def test_copy_object_default_storage_class(self):
        """Test object copy with default storage class"""
        self.s3.copy_object(
            Bucket="dest-bucket",
            Key="dest-key",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"}
        )

        self.s3.client.copy_object.assert_called_with(
            Bucket="dest-bucket",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"},
            Key="dest-key",
            StorageClass="GLACIER"
        )

    def test_copy_object_error(self):
        """Test object copy error"""
        self.s3.client.copy_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "copy_object"
        )

        with pytest.raises(ActionError):
            self.s3.copy_object(
                Bucket="dest-bucket",
                Key="dest-key",
                CopySource={"Bucket": "src-bucket", "Key": "src-key"}
            )


class TestS3ClientFactory:
    """Test s3_client_factory function"""

    def test_factory_aws(self):
        """Test factory returns AwsS3Client for aws provider"""
        with patch('boto3.client'):
            client = s3_client_factory("aws")
            assert isinstance(client, AwsS3Client)

    def test_factory_gcp_not_implemented(self):
        """Test factory raises NotImplementedError for gcp provider"""
        with pytest.raises(NotImplementedError, match="GCP S3Client is not implemented"):
            s3_client_factory("gcp")

    def test_factory_azure_not_implemented(self):
        """Test factory raises NotImplementedError for azure provider"""
        with pytest.raises(NotImplementedError, match="Azure S3Client is not implemented"):
            s3_client_factory("azure")

    def test_factory_unknown_provider(self):
        """Test factory raises ValueError for unknown provider"""
        with pytest.raises(ValueError, match="Unsupported provider"):
            s3_client_factory("unknown")


# Legacy tests for backward compatibility
def test_create_bucket():
    s3 = AwsS3Client()
    s3.client = MagicMock()
    s3.bucket_exists = MagicMock(return_value=False)  # Mock the method directly

    assert s3.bucket_exists("test-bucket") is False

    # FIXME: This test is not working as expected. Something in the way it's mocked up
    # FIXME: means that the call to create_bucket gets a different result when
    # bucket_exists() is called.
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
    # S3Client is abstract and cannot be instantiated
    with pytest.raises(TypeError):
        S3Client()