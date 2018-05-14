import click
from curator.singletons import cli

def main():
    # This is because click uses decorators, and pylint doesn't catch that
    # pylint: disable=E1120
    # pylint: disable=E1123
    cli(obj={})
