"""Helper classes for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import json
import logging
from dataclasses import dataclass
from datetime import datetime

from elasticsearch import Elasticsearch

from .constants import STATUS_INDEX


class Deepfreeze:
    """
    Allows nesting of actions under the deepfreeze command
    """


@dataclass
class ThawedRepo:
    """
    ThawedRepo is a data class representing a thawed repository and its indices.

    Attributes:
        repo_name (str): The name of the repository.
        bucket_name (str): The name of the bucket where the repository is stored.
        base_path (str): The base path of the repository.
        provider (str): The provider of the repository, default is "aws".
        indices (list): A list of indices associated with the repository.

    Methods:
        __init__(repo_info: dict, indices: list[str] = None) -> None:
            Initializes a ThawedRepo instance with repository information and optional indices.

        add_index(index: str) -> None:
            Adds an index to the list of indices.

    Example:
        thawed_repo = ThawedRepo(repo_info, indices)
        thawed_repo.add_index("index_name")
    """

    repo_name: str
    bucket_name: str
    base_path: str
    provider: str
    indices: list = None

    def __init__(self, repo_info: dict, indices: list[str] = None) -> None:
        self.repo_name = repo_info["name"]
        self.bucket_name = repo_info["bucket"]
        self.base_path = repo_info["base_path"]
        self.provider = "aws"
        self.indices = indices

    def add_index(self, index: str) -> None:
        """
        Add an index to the list of indices

        Params:
            index (str): The index to add

        Returns:
            None
        """
        self.indices.append(index)


@dataclass
class ThawSet(dict[str, ThawedRepo]):
    """
    Data class for thaw settings

    Attributes:
        doctype (str): The document type of the thaw settings.

    Methods:
        add(thawed_repo: ThawedRepo) -> None:
            Add a thawed repo to the dictionary

    Example:
        thawset = ThawSet()
        thawset.add(ThawedRepo(repo_info, indices))
    """

    doctype: str = "thawset"

    def add(self, thawed_repo: ThawedRepo) -> None:
        """
        Add a thawed repo to the dictionary

        Params:
            thawed_repo (ThawedRepo): The thawed repo to add

        Returns:
            None
        """
        self[thawed_repo.repo_name] = thawed_repo


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
    # These default datetimes are to prevent issues with None.
    start: datetime = datetime.now()
    end: datetime = datetime.now()
    is_thawed: bool = False
    is_mounted: bool = True
    doctype: str = "repository"

    def __init__(self, repo_hash=None, es: Elasticsearch = None, name=None) -> None:
        if name is not None:
            if es is not None:
                query = {"query": {"match": {"name": name}}}
                result = es.search(index=STATUS_INDEX, body=query)
                if result["hits"]["total"]["value"] > 0:
                    repo_hash = result["hits"]["hits"][0]["_source"]
                else:
                    repo_hash = {"name": name}
        if repo_hash is not None:
            for key, value in repo_hash.items():
                setattr(self, key, value)

    def to_dict(self) -> dict:
        """
        Convert the Repository object to a dictionary.
        Convert datetime to ISO 8601 string format for JSON compatibility.

        Params:
            None

        Returns:
            dict: A dictionary representation of the Repository object.
        """
        start_str = self.start.isoformat() if self.start else None
        end_str = self.end.isoformat() if self.end else None
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
        es.index(index=STATUS_INDEX, id=self.name, body=self.to_dict())


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
