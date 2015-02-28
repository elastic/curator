import click
from .cli import cli

import logging
logger = logging.getLogger(__name__)

def main():
    # Run the CLI!
    cli(obj={"filtered": [], "add_indices": []})
