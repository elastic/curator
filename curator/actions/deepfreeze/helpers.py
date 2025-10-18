"""Helper classes for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from elasticsearch import Elasticsearch

from .constants import STATUS_INDEX


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
        thaw_state (str): Lifecycle state - "frozen", "thawing", "thawed", "expired"
        thawed_at (datetime): When S3 restore completed (thawing -> thawed transition).
        expires_at (datetime): When S3 restore will/did expire.
        doctype (str): The document type of the repository.
        id [str]: The ID of the repository in Elasticsearch.

    Lifecycle States:
        frozen: Normal state, in Glacier, not thawed
        thawing: S3 restore in progress, waiting for retrieval
        thawed: S3 restore complete, mounted and in use
        expired: S3 restore expired, reverted to Glacier, ready for cleanup

    State Transitions:
        frozen -> thawing: When thaw request initiated
        thawing -> thawed: When S3 restore completes and repo is mounted
        thawed -> expired: When S3 restore expiry time passes
        expired -> frozen: When cleanup runs

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
    thaw_state: str = "frozen"  # frozen, thawing, thawed, expired
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

        # Backward compatibility: sync is_thawed with thaw_state
        if self.is_thawed and self.thaw_state == "frozen":
            # Old docs that only have is_thawed=True should be "thawed"
            self.thaw_state = "thawed" if self.is_mounted else "thawing"

    @classmethod
    def from_elasticsearch(
        cls, client: Elasticsearch, name: str, index: str = STATUS_INDEX
    ) -> Optional['Repository']:
        """
        Fetch a document from Elasticsearch by name and create a Repository instance.

        Args:
            name: The name of the repository to fetch
            client: Elasticsearch client instance
            index: The Elasticsearch index to query (default: 'repositories')

        Returns:
            Repository instance or None if not found
        """
        try:
            # Query Elasticsearch for a document matching the name
            logging.debug(f"Fetching Repository from Elasticsearch: {name}")
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
            id = hits[0]['_id']

            logging.debug(f"Document fetched: {doc}")

            # Create and return a new Repository instance
            return cls(**doc, docid=id)

        except Exception as e:
            print(f"Error fetching Repository from Elasticsearch: {e}")
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
        logging.debug(f"Repository start: {self.start}")
        logging.debug(f"Repository end: {self.end}")
        # Convert datetime objects to ISO strings for proper storage
        start_str = self.start.isoformat() if isinstance(self.start, datetime) else self.start
        end_str = self.end.isoformat() if isinstance(self.end, datetime) else self.end
        thawed_at_str = self.thawed_at.isoformat() if isinstance(self.thawed_at, datetime) else self.thawed_at
        expires_at_str = self.expires_at.isoformat() if isinstance(self.expires_at, datetime) else self.expires_at

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
        from .constants import THAW_STATE_THAWING
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
        from .constants import THAW_STATE_THAWED
        from datetime import datetime, timezone
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
        from .constants import THAW_STATE_EXPIRED
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
        from .constants import THAW_STATE_FROZEN
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
        logging.debug(f"Repository name: {self.name}")
        logging.debug(f"Repository id: {self.docid}")
        logging.debug(f"Repository body: {self.to_dict()}")
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
        thaw_request_retention_days_completed (int): Days to retain completed thaw requests.
        thaw_request_retention_days_failed (int): Days to retain failed thaw requests.

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
    thaw_request_retention_days_completed: int = 7
    thaw_request_retention_days_failed: int = 30

    def __init__(
        self,
        settings_hash: dict[str, str] = None,
        repo_name_prefix: str = "deepfreeze",
        bucket_name_prefix: str = "deepfreeze",
        base_path_prefix: str = "snapshots",
        canned_acl: str = "private",
        storage_class: str = "intelligent_tiering",
        provider: str = "aws",
        rotate_by: str = "path",
        style: str = "oneup",
        last_suffix: str = None,
        thaw_request_retention_days_completed: int = 7,
        thaw_request_retention_days_failed: int = 30,
    ) -> None:
        if settings_hash is not None:
            for key, value in settings_hash.items():
                setattr(self, key, value)
        if repo_name_prefix:
            self.repo_name_prefix = repo_name_prefix
        if bucket_name_prefix:
            self.bucket_name_prefix = bucket_name_prefix
        if base_path_prefix:
            self.base_path_prefix = base_path_prefix
        if canned_acl:
            self.canned_acl = canned_acl
        if storage_class:
            self.storage_class = storage_class
        if provider:
            self.provider = provider
        if rotate_by:
            self.rotate_by = rotate_by
        if style:
            self.style = style
        if last_suffix:
            self.last_suffix = last_suffix
        if thaw_request_retention_days_completed:
            self.thaw_request_retention_days_completed = thaw_request_retention_days_completed
        if thaw_request_retention_days_failed:
            self.thaw_request_retention_days_failed = thaw_request_retention_days_failed
