#!/usr/bin/env python

"""
Wrapper for running curator from source.

When used with Python 3 (and the DEB and RPM packages of Curator are compiled
and bundled with Python 3), Curator requires the locale to be unicode. Any of
the above unicode definitions are acceptable.

To set the locale to be unicode, try:

$ export LC_ALL=en_US.utf8
$ curator [ARGS]

Alternately, you should be able to specify the locale on the command-line:

$ LC_ALL=en_US.utf8 curator [ARGS]

Be sure to substitute your unicode variant for en_US.utf8

"""

from curator.cli import cli


if __name__ == '__main__':
    try:
        # This is because click uses decorators, and pylint doesn't catch that
        # pylint: disable=no-value-for-parameter
        cli()
    except RuntimeError as e:
        import sys
        print('{0}'.format(e))
        sys.exit(1)
    except Exception as e:
        if 'ASCII' in str(e):
            print('{0}'.format(e))
            print(__doc__)
