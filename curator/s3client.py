"""
s3client.py

Re-exports S3 client from deepfreeze-core package.
The canonical implementation lives in the 'deepfreeze_core' package.

This module is kept for backward compatibility with existing curator code.
"""

from deepfreeze_core import (
    AwsS3Client,
    S3Client,
    s3_client_factory,
)

__all__ = [
    "AwsS3Client",
    "S3Client",
    "s3_client_factory",
]
