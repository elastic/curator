"""
Deepfreeze - Standalone Elasticsearch S3 Glacier archival tool

This package provides cost-effective S3 Glacier archival and lifecycle management
for Elasticsearch snapshot repositories without requiring full Curator installation.
"""

__version__ = "1.0.0"

from deepfreeze.exceptions import (
    DeepfreezeException,
    MissingIndexError,
    MissingSettingsError,
    ActionException,
    PreconditionError,
    RepositoryException,
    ActionError,
)
from deepfreeze.constants import (
    STATUS_INDEX,
    SETTINGS_ID,
    PROVIDERS,
    THAW_STATE_ACTIVE,
    THAW_STATE_FROZEN,
    THAW_STATE_THAWING,
    THAW_STATE_THAWED,
    THAW_STATE_EXPIRED,
    THAW_STATES,
    THAW_STATUS_IN_PROGRESS,
    THAW_STATUS_COMPLETED,
    THAW_STATUS_FAILED,
    THAW_STATUS_REFROZEN,
    THAW_REQUEST_STATUSES,
)
from deepfreeze.helpers import (
    Deepfreeze,
    Repository,
    Settings,
)
from deepfreeze.s3client import (
    S3Client,
    AwsS3Client,
    s3_client_factory,
)
from deepfreeze.esclient import (
    create_es_client,
    create_es_client_from_config,
    load_config_from_yaml,
    validate_connection,
    ESClientWrapper,
)

__all__ = [
    "__version__",
    # Exceptions
    "DeepfreezeException",
    "MissingIndexError",
    "MissingSettingsError",
    "ActionException",
    "PreconditionError",
    "RepositoryException",
    "ActionError",
    # Constants
    "STATUS_INDEX",
    "SETTINGS_ID",
    "PROVIDERS",
    "THAW_STATE_ACTIVE",
    "THAW_STATE_FROZEN",
    "THAW_STATE_THAWING",
    "THAW_STATE_THAWED",
    "THAW_STATE_EXPIRED",
    "THAW_STATES",
    "THAW_STATUS_IN_PROGRESS",
    "THAW_STATUS_COMPLETED",
    "THAW_STATUS_FAILED",
    "THAW_STATUS_REFROZEN",
    "THAW_REQUEST_STATUSES",
    # Helper classes
    "Deepfreeze",
    "Repository",
    "Settings",
    # S3 Client
    "S3Client",
    "AwsS3Client",
    "s3_client_factory",
    # ES Client
    "create_es_client",
    "create_es_client_from_config",
    "load_config_from_yaml",
    "validate_connection",
    "ESClientWrapper",
]
