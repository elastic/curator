"""Refreeze action for deepfreeae"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.helpers import ThawSet
from curator.actions.deepfreeze.utilities import get_settings


class Refreeze:
    """
    First unmount a repo, then refreeze it requested (or let it age back to Glacier
    naturally)

    :param client: A client connection object
    :type client: Elasticsearch
    :param thawset: The thawset to refreeze
    :type thawset: str

    :methods:
        do_dry_run: Perform a dry-run of the refreezing process.
        do_action: Perform high-level repo refreezing steps in sequence.
    """

    def __init__(self, client: Elasticsearch, thawset: str) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.thawset = ThawSet(thawset)

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the refreezing process.

        :return: None
        :rtype: None
        """
        pass

    def do_action(self) -> None:
        """
        Perform high-level repo refreezing steps in sequence.

        :return: None
        :rtype: None
        """
        pass
