import click
from .cli import es_repo_mgr

import logging
logger = logging.getLogger(__name__)

def main():
    es_repo_mgr.repomgrcli()
