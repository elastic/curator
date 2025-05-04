"""Helper classes for deepfreeae"""

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
        is_thawed (bool): Whether the repository is thawed.
        is_mounted (bool): Whether the repository is mounted.
        doctype (str): The document type of the repository.
        id [str]: The ID of the repository in Elasticsearch.

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
    is_thawed: bool = False
    is_mounted: bool = True
    doctype: str = "repository"
    docid: str = None

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
        start_str = self.start if self.start else None
        end_str = self.end if self.end else None
        return {
            "name": self.name,
            "bucket": self.bucket,
            "base_path": self.base_path,
            "start": start_str,
            "end": end_str,
            "is_thawed": self.is_thawed,
            "is_mounted": self.is_mounted,
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
        es.update(index=STATUS_INDEX, id=self.docid, doc=self.to_dict())


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
