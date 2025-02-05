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

        Returns:
            None
        """
        raise NotImplementedError("Subclasses should implement this method")

    def refreeze(self, bucket_name: str, path: str) -> None:
        """
        Return a bucket to deepfreeze.

        Args:
            bucket_name (str): The name of the bucket to return.
            path (str): The path to the bucket to return.

        Returns:
            None
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
        try:
            self.client.create_bucket(Bucket=bucket_name)
        except ClientError as e:
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

        :param bucket_name: The name of the bucket
        :param base_path: The base path within the bucket
        :param object_keys: A list of object keys to restore
        :param restore_days: The number of days to keep the object restored
        :param retrieval_tier: The retrieval tier to use
        :return: None
        """
        self.loggit.info(f"Thawing bucket: {bucket_name} at path: {base_path}")
        for key in object_keys:
            if not key.startswith(base_path):
                continue  # Skip objects outside the base path

            try:
                response = self.client.head_object(Bucket=bucket_name, Key=key)
                storage_class = response.get("StorageClass", "")

                if storage_class in ["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"]:
                    self.loggit.info(
                        f"Restoring: {key} (Storage Class: {storage_class})"
                    )
                    self.client.restore_object(
                        Bucket=bucket_name,
                        Key=key,
                        RestoreRequest={
                            "Days": restore_days,
                            "GlacierJobParameters": {"Tier": retrieval_tier},
                        },
                    )
                else:
                    self.loggit.info(
                        f"Skipping: {key} (Storage Class: {storage_class})"
                    )

            except Exception as e:
                self.loggit.error(f"Error restoring {key}: {str(e)}")

    def refreeze(self, bucket_name: str, path: str) -> None:
        self.loggit.info(f"Refreezing bucket: {bucket_name} at path: {path}")
        # Placeholder for refreezing an AWS S3 bucket


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
