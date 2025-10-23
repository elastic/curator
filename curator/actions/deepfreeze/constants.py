"""Constants for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

STATUS_INDEX = "deepfreeze-status"
SETTINGS_ID = "1"
PROVIDERS = ["aws"]

# Repository thaw lifecycle states
THAW_STATE_ACTIVE = "active"      # Active repository, never been through thaw lifecycle
THAW_STATE_FROZEN = "frozen"      # In cold storage (Glacier), not currently accessible
THAW_STATE_THAWING = "thawing"    # S3 restore in progress, waiting for retrieval
THAW_STATE_THAWED = "thawed"      # S3 restore complete, mounted and in use
THAW_STATE_EXPIRED = "expired"    # S3 restore expired, reverted to Glacier, ready for cleanup

THAW_STATES = [
    THAW_STATE_ACTIVE,
    THAW_STATE_FROZEN,
    THAW_STATE_THAWING,
    THAW_STATE_THAWED,
    THAW_STATE_EXPIRED,
]

# Thaw request status lifecycle
THAW_STATUS_IN_PROGRESS = "in_progress"  # Thaw operation is actively running
THAW_STATUS_COMPLETED = "completed"      # Thaw completed, data available and mounted
THAW_STATUS_FAILED = "failed"            # Thaw operation failed
THAW_STATUS_REFROZEN = "refrozen"        # Thaw was completed but has been refrozen (cleaned up)

THAW_REQUEST_STATUSES = [
    THAW_STATUS_IN_PROGRESS,
    THAW_STATUS_COMPLETED,
    THAW_STATUS_FAILED,
    THAW_STATUS_REFROZEN,
]
