"""
s3client.py

import boto3

Encapsulate the S3 client here so it can be used by all Curator classes, not just
deepfreeze.
"""

import logging

import boto3
from botocore.exceptions import ClientError

from curator.exceptions import ActionError

# from botocore.exceptions import ClientError


class S3Client:
    """
    Superclass for S3 Clients.

    This class should *only* perform actions that are common to all S3 clients. It
    should not handle record-keeping or anything unrelated to S3 actions. The calling
    methods should handle that.
    """

    def create_bucket(self, bucket_name: str) -> None:
        """
        Create a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to create.

        Returns:
            None
        """
        raise NotImplementedError("Subclasses should implement this method")

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Test whether or not the named bucket exists

        :param bucket_name: Bucket name to check
        :type bucket_name: str
        :return: Existence state of named bucket
        :rtype: bool
        """
        raise NotImplementedError("Subclasses should implement this method")

    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: list[str],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Return a bucket from deepfreeze.

        Args:
            bucket_name (str): The name of the bucket to return.
            path (str): The path to the bucket to return.
            object_keys (list[str]): A list of object keys to return.
            restore_days (int): The number of days to keep the object restored.
            retrieval_tier (str): The retrieval tier to use.

        Returns:
            None
        """
        raise NotImplementedError("Subclasses should implement this method")

    def refreeze(
        self, bucket_name: str, path: str, storage_class: str = "GLACIER"
    ) -> None:
        """
        Return a bucket to deepfreeze.

        Args:
            bucket_name (str): The name of the bucket to return.
            path (str): The path to the bucket to return.
            storage_class (str): The storage class to send the data to.

        """
        raise NotImplementedError("Subclasses should implement this method")

    def list_objects(self, bucket_name: str, prefix: str) -> list[str]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): The name of the bucket to list objects from.
            prefix (str): The prefix to use when listing objects.

        Returns:
            list[str]: A list of object keys.
        """
        raise NotImplementedError("Subclasses should implement this method")

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to delete.

        Returns:
            None
        """
        raise NotImplementedError("Subclasses should implement this method")

    def put_object(self, bucket_name: str, key: str, body: str = "") -> None:
        """
        Put an object in a bucket at the given path.

        Args:
            bucket_name (str): The name of the bucket to put the object in.
            key (str): The key of the object to put.
            body (str): The body of the object to put.

        Returns:
            None
        """
        raise NotImplementedError("Subclasses should implement this method")

    def list_buckets(self, prefix: str = None) -> list[str]:
        """
        List all buckets.

        Returns:
            list[str]: A list of bucket names.
        """
        raise NotImplementedError("Subclasses should implement this method")


class AwsS3Client(S3Client):
    """
    An S3 client object for use with AWS.
    """

    def __init__(self) -> None:
        self.client = boto3.client("s3")
        self.loggit = logging.getLogger("AWS S3 Client")

    def create_bucket(self, bucket_name: str) -> None:
        self.loggit.info(f"Creating bucket: {bucket_name}")
        if self.bucket_exists(bucket_name):
            self.loggit.info(f"Bucket {bucket_name} already exists")
            raise ActionError(f"Bucket {bucket_name} already exists")
        try:
            self.client.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(f"Error creating bucket {bucket_name}: {e}")

    def bucket_exists(self, bucket_name: str) -> bool:
        self.loggit.info(f"Checking if bucket {bucket_name} exists")
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                self.loggit.error(e)
                raise ActionError(e)

    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: list[str],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Restores objects from Glacier storage class back to an instant access tier.

        Args:
            bucket_name (str): The name of the bucket
            base_path (str): The base path (prefix) of the objects to thaw
            object_keys (list[str]): A list of object keys to thaw
            restore_days (int): The number of days to keep the object restored
            retrieval_tier (str): The retrieval tier to use

        Returns:
            None
        """
        self.loggit.info(f"Thawing bucket: {bucket_name} at path: {base_path}")
        for key in object_keys:
            if not key.startswith(base_path):
                continue  # Skip objects outside the base path

            # ? Do we need to keep track of what tier this came from instead of just assuming Glacier?
            try:
                response = self.client.head_object(Bucket=bucket_name, Key=key)
                storage_class = response.get("StorageClass", "")

                if storage_class in ["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"]:
                    self.loggit.debug(f"Restoring: {key} from {storage_class})")
                    self.client.restore_object(
                        Bucket=bucket_name,
                        Key=key,
                        RestoreRequest={
                            "Days": restore_days,
                            "GlacierJobParameters": {"Tier": retrieval_tier},
                        },
                    )
                else:
                    self.loggit.debug(
                        f"Skipping: {key} (Storage Class: {storage_class})"
                    )

            except Exception as e:
                self.loggit.error(f"Error restoring {key}: {str(e)}")

    def refreeze(
        self, bucket_name: str, path: str, storage_class: str = "GLACIER"
    ) -> None:
        """
        Moves objects back to a Glacier-tier storage class.

        Args:
            bucket_name (str): The name of the bucket
            path (str): The path to the objects to refreeze
            storage_class (str): The storage class to move the objects to

        Returns:
            None
        """
        self.loggit.info(f"Refreezing objects in bucket: {bucket_name} at path: {path}")

        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=path)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]

                    try:
                        # Copy the object with a new storage class
                        self.client.copy_object(
                            Bucket=bucket_name,
                            CopySource={"Bucket": bucket_name, "Key": key},
                            Key=key,
                            StorageClass=storage_class,
                            MetadataDirective="COPY",
                        )
                        self.loggit.info(f"Refrozen: {key} to {storage_class}")

                    except Exception as e:
                        self.loggit.error(f"Error refreezing {key}: {str(e)}")

    def list_objects(self, bucket_name: str, prefix: str) -> list[str]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): The name of the bucket to list objects from.
            prefix (str): The prefix to use when listing objects.

        Returns:
            list[str]: A list of object keys.
        """
        self.loggit.info(
            f"Listing objects in bucket: {bucket_name} with prefix: {prefix}"
        )
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        object_keys = []

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    object_keys.append(obj["Key"])

        return object_keys

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to delete.

        Returns:
            None
        """
        self.loggit.info(f"Deleting bucket: {bucket_name}")
        try:
            self.client.delete_bucket(Bucket=bucket_name)
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)

    def put_object(self, bucket_name: str, key: str, body: str = "") -> None:
        """
        Put an object in a bucket.

        Args:
            bucket_name (str): The name of the bucket to put the object in.
            key (str): The key of the object to put.
            body (str): The body of the object to put.

        Returns:
            None
        """
        self.loggit.info(f"Putting object: {key} in bucket: {bucket_name}")
        try:
            self.client.put_object(Bucket=bucket_name, Key=key, Body=body)
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)

    def list_buckets(self, prefix: str = None) -> list[str]:
        """
        List all buckets.

        Returns:
            list[str]: A list of bucket names.
        """
        self.loggit.info("Listing buckets")
        try:
            response = self.client.list_buckets()
            buckets = response.get("Buckets", [])
            bucket_names = [bucket["Name"] for bucket in buckets]
            if prefix:
                bucket_names = [
                    name for name in bucket_names if name.startswith(prefix)
                ]
            return bucket_names
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)


def s3_client_factory(provider: str) -> S3Client:
    """
    s3_client_factory method, returns an S3Client object implemented specific to
    the value of the provider argument.

    Args:
        provider (str): The provider to use for the S3Client object. Should
                        reference an implemented provider (aws, gcp, azure, etc)

    Raises:
        NotImplementedError: raised if the provider is not implemented
        ValueError: raised if the provider string is invalid.

    Returns:
        S3Client: An S3Client object specific to the provider argument.
    """
    if provider == "aws":
        return AwsS3Client()
    elif provider == "gcp":
        # Placeholder for GCP S3Client implementation
        raise NotImplementedError("GCP S3Client is not implemented yet")
    elif provider == "azure":
        # Placeholder for Azure S3Client implementation
        raise NotImplementedError("Azure S3Client is not implemented yet")
    else:
        raise ValueError(f"Unsupported provider: {provider}")
