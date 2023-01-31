#!/usr/bin/env python
# pylint: disable=no-value-for-parameter, broad-except
"""
Wrapper for running singletons from source.

When used with Python 3 Curator requires the locale to be unicode. Any unicode
definitions are acceptable.

To set the locale to be unicode, try:

$ export LC_ALL=en_US.utf8
$ curator_cli [ARGS]

Alternately, you should be able to specify the locale on the command-line:

$ LC_ALL=en_US.utf8 curator_cli [ARGS]

Be sure to substitute your unicode variant for en_US.utf8
"""
import sys
import click
from curator.singletons import cli

if __name__ == '__main__':
    try:
        cli(obj={})
    except RuntimeError as err:
        click.echo(f'{err}')
        sys.exit(1)
    except Exception as err:
        if 'ASCII' in str(err):
            click.echo(f'{err}')
            click.echo(__doc__)
