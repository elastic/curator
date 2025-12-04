"""Helper classes for deepfreeze

This module contains dataclasses and helper classes for the standalone deepfreeze package.
These are extracted from the original deepfreeze helpers with dependencies removed.
"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from elasticsearch8 import Elasticsearch

from deepfreeze.constants import (
    STATUS_INDEX,
    THAW_STATE_ACTIVE,
    THAW_STATE_FROZEN,
    THAW_STATE_THAWING,
    THAW_STATE_THAWED,
    THAW_STATE_EXPIRED,
)


class Deepfreeze:
    """
    Allows nesting of actions under the deepfreeze command
    """


@dataclass
class Repository:
    """
    Data class for repository. Given a name, it will retrieve the repository from the
    status index. If given other parameters, it will create a new repository object.

    Attributes:
        name (str): The name of the repository.
        bucket (str): The name of the bucket.
        base_path (str): The base path of the repository.
        start (datetime): The start date of the repository.
        end (datetime): The end date of the repository.
        is_thawed (bool): Whether the repository is thawed (DEPRECATED - use thaw_state).
        is_mounted (bool): Whether the repository is mounted.
        thaw_state (str): Lifecycle state - "active", "frozen", "thawing", "thawed", "expired"
        thawed_at (datetime): When S3 restore completed (thawing -> thawed transition).
        expires_at (datetime): When S3 restore will/did expire.
        doctype (str): The document type of the repository.
        id [str]: The ID of the repository in Elasticsearch.

    Lifecycle States:
        active: Active repository, never been through thaw lifecycle
        frozen: In cold storage (Glacier), not currently accessible
        thawing: S3 restore in progress, waiting for retrieval
        thawed: S3 restore complete, mounted and in use
        expired: S3 restore expired, reverted to Glacier, ready for cleanup

    State Transitions:
        active -> frozen: When repository is moved to cold storage (future feature)
        frozen -> thawing: When thaw request initiated
        thawing -> thawed: When S3 restore completes and repo is mounted
        thawed -> expired: When S3 restore expiry time passes
        expired -> frozen: When cleanup runs (refreeze operation)

    Methods:
        to_dict() -> dict:
            Convert the Repository object to a dictionary.

        to_json() -> str:
            Convert the Repository object to a JSON string.

        __lt__(other) -> bool:
            Less than comparison based on the repository name.

        persist(es: Elasticsearch) -> None:
            Persist the repository to the status index.

    Example:
        repo = Repository({name="repo1", bucket="bucket1", base_path="path1", start=datetime.now(), end=datetime.now()})
        repo = Repository(name="deepfreeze-000032")
        repo_dict = repo.to_dict()
        repo_json = repo.to_json()
    """

    name: str = None
    bucket: str = None
    base_path: str = None
    start: datetime = None
    end: datetime = None
    is_thawed: bool = False  # DEPRECATED - use thaw_state instead
    is_mounted: bool = True
    thaw_state: str = THAW_STATE_ACTIVE  # active, frozen, thawing, thawed, expired
    thawed_at: datetime = None  # When restore completed
    expires_at: datetime = None  # When restore will/did expire
    doctype: str = "repository"
    docid: str = None

    def __post_init__(self):
        """Convert string dates from Elasticsearch to datetime objects"""
        if isinstance(self.start, str):
            self.start = datetime.fromisoformat(self.start)
        if isinstance(self.end, str):
            self.end = datetime.fromisoformat(self.end)
        if isinstance(self.thawed_at, str):
            self.thawed_at = datetime.fromisoformat(self.thawed_at)
        if isinstance(self.expires_at, str):
            self.expires_at = datetime.fromisoformat(self.expires_at)

    @classmethod
    def from_elasticsearch(
        cls, client: Elasticsearch, name: str, index: str = STATUS_INDEX
    ) -> Optional['Repository']:
        """
        Fetch a document from Elasticsearch by name and create a Repository instance.

        Args:
            name: The name of the repository to fetch
            client: Elasticsearch client instance
            index: The Elasticsearch index to query (default: 'deepfreeze-status')

        Returns:
            Repository instance or None if not found
        """
        try:
            # Query Elasticsearch for a document matching the name
            logging.debug("Fetching Repository from Elasticsearch: %s", name)
            response = client.search(
                index=index,
                query={"match": {"name.keyword": name}},  # Use .keyword for exact match
                size=1,
            )

            # Check if we got any hits
            hits = response['hits']['hits']
            if not hits:
                return None

            # Extract the document source
            doc = hits[0]['_source']
            doc_id = hits[0]['_id']

            logging.debug("Document fetched: %s", doc)

            # Create and return a new Repository instance
            return cls(**doc, docid=doc_id)

        except Exception as e:
            logging.error(
                "Error fetching Repository from Elasticsearch: %s (type: %s)",
                e,
                type(e).__name__,
                exc_info=True,
            )
            return None

    def to_dict(self) -> dict:
        """
        Convert the Repository object to a dictionary.
        Convert datetime to ISO 8601 string format for JSON compatibility.

        Params:
            None

        Returns:
            dict: A dictionary representation of the Repository object.
        """
        logging.debug("Converting Repository to dict")
        logging.debug("Repository start: %s", self.start)
        logging.debug("Repository end: %s", self.end)
        # Convert datetime objects to ISO strings for proper storage
        start_str = (
            self.start.isoformat() if isinstance(self.start, datetime) else self.start
        )
        end_str = self.end.isoformat() if isinstance(self.end, datetime) else self.end
        thawed_at_str = (
            self.thawed_at.isoformat()
            if isinstance(self.thawed_at, datetime)
            else self.thawed_at
        )
        expires_at_str = (
            self.expires_at.isoformat()
            if isinstance(self.expires_at, datetime)
            else self.expires_at
        )

        return {
            "name": self.name,
            "bucket": self.bucket,
            "base_path": self.base_path,
            "start": start_str,
            "end": end_str,
            "is_thawed": self.is_thawed,  # Keep for backward compatibility
            "is_mounted": self.is_mounted,
            "thaw_state": self.thaw_state,
            "thawed_at": thawed_at_str,
            "expires_at": expires_at_str,
            "doctype": self.doctype,
        }

    def unmount(self) -> None:
        """
        Unmount the repository by setting is_mounted to False.

        Params:
            None

        Returns:
            None
        """
        self.is_mounted = False

    def start_thawing(self, expires_at: datetime) -> None:
        """
        Transition repository to 'thawing' state when S3 restore is initiated.

        Params:
            expires_at (datetime): When the S3 restore will expire

        Returns:
            None
        """
        self.thaw_state = THAW_STATE_THAWING
        self.expires_at = expires_at
        self.is_thawed = True  # Maintain backward compatibility

    def mark_thawed(self) -> None:
        """
        Transition repository to 'thawed' state when S3 restore completes and repo is mounted.

        Params:
            None

        Returns:
            None
        """
        self.thaw_state = THAW_STATE_THAWED
        self.thawed_at = datetime.now(timezone.utc)
        self.is_thawed = True  # Maintain backward compatibility
        self.is_mounted = True

    def mark_expired(self) -> None:
        """
        Transition repository to 'expired' state when S3 restore has expired.

        Params:
            None

        Returns:
            None
        """
        self.thaw_state = THAW_STATE_EXPIRED
        # Keep thawed_at and expires_at for historical tracking

    def reset_to_frozen(self) -> None:
        """
        Transition repository back to 'frozen' state after cleanup.

        Params:
            None

        Returns:
            None
        """
        self.thaw_state = THAW_STATE_FROZEN
        self.is_thawed = False  # Maintain backward compatibility
        self.is_mounted = False
        self.thawed_at = None
        self.expires_at = None

    def to_json(self) -> str:
        """
        Convert the Repository object to a JSON string.

        Params:
            None

        Returns:
            str: A JSON string representation of the Repository object.
        """
        return json.dumps(self.to_dict(), indent=4)

    def __lt__(self, other):
        """
        Less than comparison based on the repository name.

        Params:
            other (Repository): Another Repository object to compare with.

        Returns:
            bool: True if this repository's name is less than the other repository's name, False otherwise.
        """
        return self.name < other.name

    def persist(self, es: Elasticsearch) -> None:
        """
        Persist the repository to the status index.

        Params:
            es (Elasticsearch): The Elasticsearch client.

        Returns:
            None
        """
        logging.debug("Persisting Repository to Elasticsearch")
        logging.debug("Repository name: %s", self.name)
        logging.debug("Repository id: %s", self.docid)
        logging.debug("Repository body: %s", self.to_dict())
        es.update(index=STATUS_INDEX, id=self.docid, body={"doc": self.to_dict()})


@dataclass
class Settings:
    """
    Data class for settings. Can be instantiated from a dictionary or from individual
    parameters.

    Attributes:
        doctype (str): The document type of the settings.
        repo_name_prefix (str): The prefix for repository names.
        bucket_name_prefix (str): The prefix for bucket names.
        base_path_prefix (str): The base path prefix.
        canned_acl (str): The canned ACL.
        storage_class (str): The storage class.
        provider (str): The provider.
        rotate_by (str): The rotation style.
        style (str): The style of the settings.
        last_suffix (str): The last suffix.
        ilm_policy_name (str): The name of the ILM policy used for deepfreeze.
        index_template_name (str): The name of the index template attached to the ILM policy.
        thaw_request_retention_days_completed (int): Days to retain completed thaw requests.
        thaw_request_retention_days_failed (int): Days to retain failed thaw requests.
        thaw_request_retention_days_refrozen (int): Days to retain refrozen thaw requests.

    """

    doctype: str = "settings"
    repo_name_prefix: str = "deepfreeze"
    bucket_name_prefix: str = "deepfreeze"
    base_path_prefix: str = "snapshots"
    canned_acl: str = "private"
    storage_class: str = "intelligent_tiering"
    provider: str = "aws"
    rotate_by: str = "path"
    style: str = "oneup"
    last_suffix: str = None
    ilm_policy_name: str = None
    index_template_name: str = None
    thaw_request_retention_days_completed: int = 7
    thaw_request_retention_days_failed: int = 30
    thaw_request_retention_days_refrozen: int = 35

    def __init__(
        self,
        settings_hash: dict = None,
        repo_name_prefix: str = None,
        bucket_name_prefix: str = None,
        base_path_prefix: str = None,
        canned_acl: str = None,
        storage_class: str = None,
        provider: str = None,
        rotate_by: str = None,
        style: str = None,
        last_suffix: str = None,
        ilm_policy_name: str = None,
        index_template_name: str = None,
        thaw_request_retention_days_completed: int = None,
        thaw_request_retention_days_failed: int = None,
        thaw_request_retention_days_refrozen: int = None,
    ) -> None:
        # Start with class-level defaults
        self.doctype = "settings"
        self.repo_name_prefix = "deepfreeze"
        self.bucket_name_prefix = "deepfreeze"
        self.base_path_prefix = "snapshots"
        self.canned_acl = "private"
        self.storage_class = "intelligent_tiering"
        self.provider = "aws"
        self.rotate_by = "path"
        self.style = "oneup"
        self.last_suffix = None
        self.ilm_policy_name = None
        self.index_template_name = None
        self.thaw_request_retention_days_completed = 7
        self.thaw_request_retention_days_failed = 30
        self.thaw_request_retention_days_refrozen = 35

        # If settings_hash is provided, apply those values (overwriting defaults)
        if settings_hash is not None:
            for key, value in settings_hash.items():
                if hasattr(self, key):
                    setattr(self, key, value)

        # Then apply any explicit keyword arguments (overwriting hash values if provided)
        if repo_name_prefix is not None:
            self.repo_name_prefix = repo_name_prefix
        if bucket_name_prefix is not None:
            self.bucket_name_prefix = bucket_name_prefix
        if base_path_prefix is not None:
            self.base_path_prefix = base_path_prefix
        if canned_acl is not None:
            self.canned_acl = canned_acl
        if storage_class is not None:
            self.storage_class = storage_class
        if provider is not None:
            self.provider = provider
        if rotate_by is not None:
            self.rotate_by = rotate_by
        if style is not None:
            self.style = style
        if last_suffix is not None:
            self.last_suffix = last_suffix
        if ilm_policy_name is not None:
            self.ilm_policy_name = ilm_policy_name
        if index_template_name is not None:
            self.index_template_name = index_template_name
        if thaw_request_retention_days_completed is not None:
            self.thaw_request_retention_days_completed = (
                thaw_request_retention_days_completed
            )
        if thaw_request_retention_days_failed is not None:
            self.thaw_request_retention_days_failed = thaw_request_retention_days_failed
        if thaw_request_retention_days_refrozen is not None:
            self.thaw_request_retention_days_refrozen = (
                thaw_request_retention_days_refrozen
            )

    def to_dict(self) -> dict:
        """
        Convert the Settings object to a dictionary.

        Returns:
            dict: A dictionary representation of the Settings object.
        """
        return {
            "doctype": self.doctype,
            "repo_name_prefix": self.repo_name_prefix,
            "bucket_name_prefix": self.bucket_name_prefix,
            "base_path_prefix": self.base_path_prefix,
            "canned_acl": self.canned_acl,
            "storage_class": self.storage_class,
            "provider": self.provider,
            "rotate_by": self.rotate_by,
            "style": self.style,
            "last_suffix": self.last_suffix,
            "ilm_policy_name": self.ilm_policy_name,
            "index_template_name": self.index_template_name,
            "thaw_request_retention_days_completed": self.thaw_request_retention_days_completed,
            "thaw_request_retention_days_failed": self.thaw_request_retention_days_failed,
            "thaw_request_retention_days_refrozen": self.thaw_request_retention_days_refrozen,
        }

    def to_json(self) -> str:
        """
        Convert the Settings object to a JSON string.

        Returns:
            str: A JSON string representation of the Settings object.
        """
        return json.dumps(self.to_dict(), indent=4)
