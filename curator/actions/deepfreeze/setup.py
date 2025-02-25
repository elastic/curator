"""Setup action for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from dataclasses import dataclass

from elasticsearch8 import Elasticsearch

from curator.exceptions import RepositoryException
from curator.s3client import s3_client_factory

from .helpers import Settings
from .utilities import (
    create_ilm_policy,
    create_repo,
    ensure_settings_index,
    get_matching_repo_names,
    save_settings,
)


class Setup:
    """
    Setup is responsible for creating the initial repository and bucket for
    deepfreeze operations.

    :param client: A client connection object
    :param repo_name_prefix: A prefix for repository names, defaults to `deepfreeze`
    :param bucket_name_prefix: A prefix for bucket names, defaults to `deepfreeze`
    :param base_path_prefix: Path within a bucket where snapshots are stored, defaults to `snapshots`
    :param canned_acl: One of the AWS canned ACL values (see
        `<https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl>`),
        defaults to `private`
    :param storage_class: AWS Storage class (see `<https://aws.amazon.com/s3/storage-classes/>`),
        defaults to `intelligent_tiering`
    :param provider: The provider to use (AWS only for now), defaults to `aws`, and will be saved
        to the deepfreeze status index for later reference.
    :param rotate_by: Rotate by bucket or path within a bucket?, defaults to `path`

    :raises RepositoryException: If a repository with the given prefix already exists

    :methods:
        do_dry_run: Perform a dry-run of the setup process.
        do_action: Perform create initial bucket and repository.

    :example:
        >>> from curator.actions.deepfreeze import Setup
        >>> setup = Setup(client, repo_name_prefix="deepfreeze", bucket_name_prefix="deepfreeze", base_path_prefix="snapshots", canned_acl="private", storage_class="intelligent_tiering", provider="aws", rotate_by="path")
        >>> setup.do_dry_run()
        >>> setup.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        year: int = None,
        month: int = None,
        repo_name_prefix: str = "deepfreeze",
        bucket_name_prefix: str = "deepfreeze",
        base_path_prefix: str = "snapshots",
        canned_acl: str = "private",
        storage_class: str = "intelligent_tiering",
        provider: str = "aws",
        rotate_by: str = "path",
        style: str = "oneup",
        create_sample_ilm_policy: bool = False,
        ilm_policy_name: str = "deepfreeze-sample-policy",
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Setup")

        self.client = client
        self.year = year
        self.month = month
        self.settings = Settings(
            repo_name_prefix=repo_name_prefix,
            bucket_name_prefix=bucket_name_prefix,
            base_path_prefix=base_path_prefix,
            canned_acl=canned_acl,
            storage_class=storage_class,
            provider=provider,
            rotate_by=rotate_by,
            style=style,
        )
        self.create_sample_ilm_policy = create_sample_ilm_policy
        self.ilm_policy_name = ilm_policy_name
        self.base_path = self.settings.base_path_prefix

        self.s3 = s3_client_factory(self.settings.provider)

        self.suffix = "000001"
        if self.settings.style != "oneup":
            self.suffix = f"{self.year:04}.{self.month:02}"
        self.settings.last_suffix = self.suffix

        self.new_repo_name = f"{self.settings.repo_name_prefix}-{self.suffix}"
        if self.settings.rotate_by == "bucket":
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}-{self.suffix}"
            self.base_path = f"{self.settings.base_path_prefix}"
        else:
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}"
            self.base_path = f"{self.base_path}-{self.suffix}"

        self.loggit.debug("Getting repo list")
        self.repo_list = get_matching_repo_names(
            self.client, self.settings.repo_name_prefix
        )
        self.repo_list.sort()
        self.loggit.debug("Repo list: %s", self.repo_list)

        if len(self.repo_list) > 0:
            raise RepositoryException(
                f"repositories matching {self.settings.repo_name_prefix}-* already exist"
            )
        self.loggit.debug("Deepfreeze Setup initialized")

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the setup process.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = f"DRY-RUN: deepfreeze setup of {self.new_repo_name} backed by {self.new_bucket_name}, with base path {self.base_path}."
        self.loggit.info(msg)
        self.loggit.info("DRY-RUN: Creating bucket %s", self.new_bucket_name)
        create_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
            dry_run=True,
        )

    def do_action(self) -> None:
        """
        Perform setup steps to create initial bucket and repository and save settings.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting Setup action")
        ensure_settings_index(self.client, create_if_missing=True)
        save_settings(self.client, self.settings)
        self.s3.create_bucket(self.new_bucket_name)
        create_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
        )
        if self.create_sample_ilm_policy:
            policy_name = self.ilm_policy_name
            policy_body = {
                "policy": {
                    "phases": {
                        "hot": {
                            "min_age": "0ms",
                            "actions": {
                                "rollover": {"max_size": "45gb", "max_age": "7d"}
                            },
                        },
                        "frozen": {
                            "min_age": "14d",
                            "actions": {
                                "searchable_snapshot": {
                                    "snapshot_repository": self.new_repo_name
                                }
                            },
                        },
                        "delete": {
                            "min_age": "365d",
                            "actions": {
                                "delete": {"delete_searchable_snapshot": False}
                            },
                        },
                    }
                }
            }
            self.loggit.info("Creating ILM policy %s", policy_name)
            self.loggit.debug("ILM policy body: %s", policy_body)
            response = create_ilm_policy(
                client=self.client, policy_name=policy_name, policy_body=policy_body
            )
        self.loggit.info(
            "Setup complete. You now need to update ILM policies to use %s.",
            self.new_repo_name,
        )
        self.loggit.info(
            "Ensure that all ILM policies using this repository have delete_searchable_snapshot set to false. "
            "See https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-delete.html"
        )
