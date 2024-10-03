"""Thaw action class"""

import logging
import re
from datetime import datetime

from dateutil import parser

from curator.exceptions import RepositoryException


class Thaw:
    """
    The Thaw action brings back a repository from the deepfreeze, and remounts
    snapshotted indices from that repo which cover the time range requested.
    """

    def __init__(
        self,
        client,
        repo_name_prefix="deepfreeze-",
        start_date=None,
        end_date=None,
    ):
        """
        :param client: A client connection object
        :param repo_name_prefix: A prefix for repository names, defaults to `deepfreeze-`
        :param start_date: The start date of the snapshot range to thaw
        :param end_date: The end date of the snapshot range to thaw
        """
        self.client = client
        self.repo_name_prefix = repo_name_prefix
        self.start_date = parser.parse(start_date)
        self.end_date = parser.parse(end_date)

        self.repo_list = self.get_repos()
        if not self.repo_list:
            raise RepositoryException("No repositories found with the given prefix.")
        self.repo_list.sort()

        self.loggit = logging.getLogger("curator.actions.thaw")

    def get_repos(self):
        """
        Get the complete list of repos and return just the ones whose names
        begin with our prefix.

        :returns:   The repos.
        :rtype:     list[object]
        """
        repos = self.client.snapshot.get_repository()
        pattern = re.compile(self.repo_name_prefix)
        return [repo for repo in repos if pattern.search(repo)]

    def find_repo_to_thaw(self):
        pass

    def remount_repo(self):
        pass

    def find_snapshots_to_thaw(self):
        pass

    def remount_snapshots(self):
        pass

    def do_dry_run(self):
        pass

    def do_action(self):
        """
        Perform high-level steps in sequence.
        """
        self.find_repo_to_thaw()
        self.remount_repo()
        self.find_snapshots_to_thaw()
        self.remount_snapshots()
