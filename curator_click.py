#!/usr/bin/env python

import click
from curator import cli

import logging
logger = logging.getLogger(__name__)

def main():
    # Run the CLI!
    cli(obj={"filtered": [], "add_indices": []})

if __name__ == '__main__':
    main()
