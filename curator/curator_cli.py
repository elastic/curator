"""CLI Wrapper used by run_curator.py"""
from curator.singletons import cli

def main():
    """Main function called by run_curator.py"""
    # This is because click uses decorators, and pylint doesn't catch that
    # pylint: disable=E1120
    # pylint: disable=E1123
    cli(obj={})
