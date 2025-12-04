"""
Tests for S3 client (Task Group 3 + Task Group 17 additions)

These tests verify that:
1. S3 client can be instantiated (with mocked boto3)
2. bucket_exists method works correctly
3. list_objects method works correctly
4. Factory function returns correct client types
5. Factory function handles invalid providers
6. AWS S3 operations work independently

Task Group 17 additions:
7. Bucket operations (create, delete)
8. Object operations (put, head, copy)
9. Thaw/refreeze operations
10. Error handling
"""

import pytest
from unittest.mock import MagicMock, patch, call


class TestAwsS3ClientInstantiation:
    """Tests for AwsS3Client instantiation"""

    @patch("deepfreeze.s3client.boto3")
    def test_client_instantiation_success(self, mock_boto3):
        """Test that AwsS3Client can be instantiated with valid credentials"""
        from deepfreeze.s3client import AwsS3Client

        # Mock successful list_buckets call
        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()

        assert client is not None
        assert client.client == mock_client
        mock_boto3.client.assert_called_once_with("s3")
        mock_client.list_buckets.assert_called_once()

    @patch("deepfreeze.s3client.boto3")
    def test_client_instantiation_invalid_credentials(self, mock_boto3):
        """Test that AwsS3Client raises ActionError for invalid credentials"""
        from deepfreeze.s3client import AwsS3Client
        from deepfreeze.exceptions import ActionError
        from botocore.exceptions import ClientError

        # Mock failed list_buckets call
        mock_client = MagicMock()
        error_response = {
            "Error": {"Code": "InvalidAccessKeyId", "Message": "Invalid key"}
        }
        mock_client.list_buckets.side_effect = ClientError(error_response, "ListBuckets")
        mock_boto3.client.return_value = mock_client

        with pytest.raises(ActionError) as exc_info:
            AwsS3Client()

        assert "AWS credentials are invalid" in str(exc_info.value)


class TestBucketExists:
    """Tests for bucket_exists method"""

    @patch("deepfreeze.s3client.boto3")
    def test_bucket_exists_true(self, mock_boto3):
        """Test bucket_exists returns True when bucket exists"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_client.head_bucket.return_value = {}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.bucket_exists("test-bucket")

        assert result is True
        mock_client.head_bucket.assert_called_once_with(Bucket="test-bucket")

    @patch("deepfreeze.s3client.boto3")
    def test_bucket_exists_false(self, mock_boto3):
        """Test bucket_exists returns False when bucket does not exist"""
        from deepfreeze.s3client import AwsS3Client
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.bucket_exists("nonexistent-bucket")

        assert result is False


class TestListObjects:
    """Tests for list_objects method"""

    @patch("deepfreeze.s3client.boto3")
    def test_list_objects_empty(self, mock_boto3):
        """Test list_objects returns empty list when no objects"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}

        # Mock paginator for list_objects
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{}]  # Empty page
        mock_client.get_paginator.return_value = mock_paginator
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.list_objects("test-bucket", "prefix/")

        assert result == []

    @patch("deepfreeze.s3client.boto3")
    def test_list_objects_with_results(self, mock_boto3):
        """Test list_objects returns objects when they exist"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}

        # Mock paginator for list_objects with results
        mock_objects = [
            {"Key": "prefix/file1.txt", "StorageClass": "STANDARD"},
            {"Key": "prefix/file2.txt", "StorageClass": "GLACIER"},
        ]
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": mock_objects}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.list_objects("test-bucket", "prefix/")

        assert len(result) == 2
        assert result[0]["Key"] == "prefix/file1.txt"
        assert result[1]["StorageClass"] == "GLACIER"


class TestS3ClientFactory:
    """Tests for s3_client_factory function"""

    @patch("deepfreeze.s3client.boto3")
    def test_factory_returns_aws_client(self, mock_boto3):
        """Test factory returns AwsS3Client for 'aws' provider"""
        from deepfreeze.s3client import s3_client_factory, AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = s3_client_factory("aws")

        assert isinstance(client, AwsS3Client)

    def test_factory_raises_not_implemented_for_gcp(self):
        """Test factory raises NotImplementedError for 'gcp' provider"""
        from deepfreeze.s3client import s3_client_factory

        with pytest.raises(NotImplementedError) as exc_info:
            s3_client_factory("gcp")

        assert "GCP S3Client is not implemented" in str(exc_info.value)

    def test_factory_raises_not_implemented_for_azure(self):
        """Test factory raises NotImplementedError for 'azure' provider"""
        from deepfreeze.s3client import s3_client_factory

        with pytest.raises(NotImplementedError) as exc_info:
            s3_client_factory("azure")

        assert "Azure S3Client is not implemented" in str(exc_info.value)

    def test_factory_raises_value_error_for_invalid_provider(self):
        """Test factory raises ValueError for invalid provider"""
        from deepfreeze.s3client import s3_client_factory

        with pytest.raises(ValueError) as exc_info:
            s3_client_factory("invalid")

        assert "Unsupported provider" in str(exc_info.value)


class TestS3ClientOperations:
    """Tests for various S3Client operations"""

    @patch("deepfreeze.s3client.boto3")
    def test_test_connection_success(self, mock_boto3):
        """Test test_connection returns True when connection succeeds"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.test_connection()

        assert result is True

    @patch("deepfreeze.s3client.boto3")
    def test_test_connection_failure(self, mock_boto3):
        """Test test_connection returns False when connection fails"""
        from deepfreeze.s3client import AwsS3Client
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        # First call succeeds (for init)
        mock_client.list_buckets.side_effect = [
            {"Buckets": []},
            ClientError({"Error": {"Code": "Error"}}, "ListBuckets"),
        ]
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.test_connection()

        assert result is False


# ============================================================================
# Task Group 17: Additional S3 Client Tests
# ============================================================================


class TestBucketOperations:
    """Tests for bucket create and delete operations (Task Group 17)"""

    @patch("deepfreeze.s3client.boto3")
    def test_create_bucket_success(self, mock_boto3):
        """Test create_bucket creates bucket successfully"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_client.head_bucket.side_effect = Exception("not found")  # bucket doesn't exist
        mock_client.meta.region_name = "us-east-1"
        mock_boto3.client.return_value = mock_client

        # Simulate bucket doesn't exist check
        from botocore.exceptions import ClientError
        mock_client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadBucket"
        )

        client = AwsS3Client()
        client.create_bucket("new-bucket")

        mock_client.create_bucket.assert_called_once_with(Bucket="new-bucket")

    @patch("deepfreeze.s3client.boto3")
    def test_create_bucket_already_exists(self, mock_boto3):
        """Test create_bucket raises error when bucket exists"""
        from deepfreeze.s3client import AwsS3Client
        from deepfreeze.exceptions import ActionError

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_client.head_bucket.return_value = {}  # bucket exists
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()

        with pytest.raises(ActionError) as exc_info:
            client.create_bucket("existing-bucket")

        assert "already exists" in str(exc_info.value)

    @patch("deepfreeze.s3client.boto3")
    def test_delete_bucket_success(self, mock_boto3):
        """Test delete_bucket deletes bucket successfully"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        client.delete_bucket("delete-me-bucket")

        mock_client.delete_bucket.assert_called_once_with(Bucket="delete-me-bucket")

    @patch("deepfreeze.s3client.boto3")
    def test_delete_bucket_force(self, mock_boto3):
        """Test delete_bucket with force=True empties bucket first"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "file1.txt"}, {"Key": "file2.txt"}]}
        ]
        mock_client.get_paginator.return_value = mock_paginator
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        client.delete_bucket("bucket-with-objects", force=True)

        # Should have called delete_objects
        mock_client.delete_objects.assert_called()
        mock_client.delete_bucket.assert_called_once_with(Bucket="bucket-with-objects")


class TestObjectOperations:
    """Tests for object put, head, and copy operations (Task Group 17)"""

    @patch("deepfreeze.s3client.boto3")
    def test_put_object_success(self, mock_boto3):
        """Test put_object successfully puts object"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        client.put_object("test-bucket", "test-key", "test-body")

        mock_client.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key", Body="test-body"
        )

    @patch("deepfreeze.s3client.boto3")
    def test_head_object_success(self, mock_boto3):
        """Test head_object returns metadata"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_client.head_object.return_value = {
            "ContentLength": 1024,
            "StorageClass": "GLACIER",
            "Restore": 'ongoing-request="false"',
        }
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.head_object("test-bucket", "test-key")

        assert result["ContentLength"] == 1024
        assert result["StorageClass"] == "GLACIER"
        mock_client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key"
        )

    @patch("deepfreeze.s3client.boto3")
    def test_copy_object_success(self, mock_boto3):
        """Test copy_object copies with storage class"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        client.copy_object(
            Bucket="dest-bucket",
            Key="dest-key",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"},
            StorageClass="GLACIER",
        )

        mock_client.copy_object.assert_called_once_with(
            Bucket="dest-bucket",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"},
            Key="dest-key",
            StorageClass="GLACIER",
        )

    @patch("deepfreeze.s3client.boto3")
    def test_list_buckets_with_prefix(self, mock_boto3):
        """Test list_buckets filters by prefix"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "deepfreeze-bucket-1"},
                {"Name": "deepfreeze-bucket-2"},
                {"Name": "other-bucket"},
            ]
        }
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        result = client.list_buckets(prefix="deepfreeze")

        assert len(result) == 2
        assert "deepfreeze-bucket-1" in result
        assert "deepfreeze-bucket-2" in result
        assert "other-bucket" not in result


class TestThawRefreezeOperations:
    """Tests for thaw and refreeze operations (Task Group 17)"""

    @patch("deepfreeze.s3client.boto3")
    def test_thaw_initiates_restore_for_glacier_objects(self, mock_boto3):
        """Test thaw initiates restore for Glacier storage class objects"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()

        object_keys = [
            {"Key": "path/file1.txt", "StorageClass": "GLACIER"},
            {"Key": "path/file2.txt", "StorageClass": "DEEP_ARCHIVE"},
            {"Key": "path/file3.txt", "StorageClass": "STANDARD"},  # Should skip
        ]

        client.thaw("test-bucket", "path/", object_keys, restore_days=7, retrieval_tier="Standard")

        # Should have called restore_object twice (only for GLACIER and DEEP_ARCHIVE)
        assert mock_client.restore_object.call_count == 2

    @patch("deepfreeze.s3client.boto3")
    def test_thaw_skips_objects_outside_base_path(self, mock_boto3):
        """Test thaw skips objects that don't start with base_path"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()

        object_keys = [
            {"Key": "path/file1.txt", "StorageClass": "GLACIER"},
            {"Key": "other/file2.txt", "StorageClass": "GLACIER"},  # Should skip
        ]

        client.thaw("test-bucket", "path/", object_keys, restore_days=7, retrieval_tier="Standard")

        # Should have called restore_object only once
        assert mock_client.restore_object.call_count == 1

    @patch("deepfreeze.s3client.boto3")
    def test_refreeze_copies_objects_to_glacier(self, mock_boto3):
        """Test refreeze copies objects to GLACIER storage class"""
        from deepfreeze.s3client import AwsS3Client

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "path/file1.txt", "StorageClass": "STANDARD"},
                    {"Key": "path/file2.txt", "StorageClass": "STANDARD"},
                ]
            }
        ]
        mock_client.get_paginator.return_value = mock_paginator
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()
        client.refreeze("test-bucket", "path/", storage_class="GLACIER")

        # Should have called copy_object twice with GLACIER storage class
        assert mock_client.copy_object.call_count == 2


class TestS3ClientErrorHandling:
    """Tests for error handling in S3 operations (Task Group 17)"""

    @patch("deepfreeze.s3client.boto3")
    def test_head_object_raises_action_error(self, mock_boto3):
        """Test head_object raises ActionError on failure"""
        from deepfreeze.s3client import AwsS3Client
        from deepfreeze.exceptions import ActionError
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
            "HeadObject"
        )
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()

        with pytest.raises(ActionError) as exc_info:
            client.head_object("test-bucket", "missing-key")

        assert "Error getting metadata" in str(exc_info.value)

    @patch("deepfreeze.s3client.boto3")
    def test_bucket_exists_raises_on_unexpected_error(self, mock_boto3):
        """Test bucket_exists raises ActionError on unexpected errors"""
        from deepfreeze.s3client import AwsS3Client
        from deepfreeze.exceptions import ActionError
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.list_buckets.return_value = {"Buckets": []}
        # Non-404 error should raise
        mock_client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "HeadBucket"
        )
        mock_boto3.client.return_value = mock_client

        client = AwsS3Client()

        with pytest.raises(ActionError):
            client.bucket_exists("access-denied-bucket")
