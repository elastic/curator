#!/usr/bin/env python

"""Wrapper for running singletons from source."""
import click
from curator.singletons import cli

if __name__ == '__main__':
    try:
        cli(obj={})
    except Exception as e:
        if type(e) == type(RuntimeError()):
            if 'ASCII' in str(e):
                print('{0}'.format(e))
                print(
'''

When used with Python 3 (and the DEB and RPM packages of Curator are compiled
and bundled with Python 3), Curator requires the locale to be unicode. Any of
the above unicode definitions are acceptable.

To set the locale to be unicode, try:

$ export LC_ALL=en_US.utf8
$ curator_cli [ARGS]

Alternately, you should be able to specify the locale on the command-line:

$ LC_ALL=en_US.utf8 curator_cli [ARGS]

Be sure to substitute your unicode variant for en_US.utf8

'''
            )
        else:
            import sys
            print('{0}'.format(e))
            sys.exit(1)
