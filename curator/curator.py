import click
from .cli import cli

import logging
logger = logging.getLogger(__name__)

def main():
    cli( obj={ "filters": [] } )
