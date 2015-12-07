import click
import sys
import re
import time
import logging
import json
from .utils import *

import elasticsearch
from ..api import *

logger = logging.getLogger(__name__)

# Elasticsearch versions supported
version_max  = (3, 0, 0)
version_min = (1, 0, 0)

REGEX_MAP = {
    'timestring': r'^.*{0}.*$',
    'newest': r'(?P<date>{0})',
    'oldest': r'(?P<date>{0})',
    'newer_than': r'(?P<date>{0})',
    'older_than': r'(?P<date>{0})',
    'prefix': r'^{0}.*$',
    'suffix': r'^.*{0}$',
}

def countdown(seconds):
    """Display an inline countdown to stdout."""
    for i in range(seconds,0,-1):
        sys.stdout.write(str(i) + ' ')
        sys.stdout.flush()
        time.sleep(1)

class LogstashFormatter(logging.Formatter):
    # The LogRecord attributes we want to carry over to the Logstash message,
    # mapped to the corresponding output key.
    WANTED_ATTRS = {'levelname': 'loglevel',
                    'funcName': 'function',
                    'lineno': 'linenum',
                    'message': 'message',
                    'name': 'name'}

    def converter(self, timevalue):
        return time.gmtime(timevalue)

    def format(self, record):
        timestamp = '%s.%03dZ' % (
            self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S'), record.msecs)
        result = {'message': record.getMessage(),
                  '@timestamp': timestamp}
        for attribute in set(self.WANTED_ATTRS).intersection(record.__dict__):
            result[self.WANTED_ATTRS[attribute]] = getattr(record, attribute)
        return json.dumps(result, sort_keys=True)

class Whitelist(logging.Filter):
    def __init__(self, *whitelist):
        self.whitelist = [logging.Filter(name) for name in whitelist]

    def filter(self, record):
        return any(f.filter(record) for f in self.whitelist)

def exit_msg(success):
    """
    Display a message corresponding to whether the job completed successfully or
    not, then exit.
    """
    if success:
        logger.info("Job completed successfully.")
    else:
        logger.warn("Job did not complete successfully.")
    sys.exit(0) if success else sys.exit(1)

def show_dry_run(client, items, command, type=None):
    """
    Log dry run output with the command which would have been executed.
    """
    logger.info("DRY RUN MODE.  No changes will be made.")
    for item in items:
        if type == 'indices':
            logger.info("DRY RUN: {0}: {1}{2}".format(command, item, ' (CLOSED)' if index_closed(client, item) else ''))
        else:
            logger.info("DRY RUN: {0}: {1}".format(command, item))

def check_version(client):
    """
    Verify version is within acceptable range.  Exit with error if it is not.

    :arg client: The Elasticsearch client connection
    """
    version_number = get_version(client)
    logger.debug('Detected Elasticsearch version {0}'.format(".".join(map(str,version_number))))
    if version_number >= version_max or version_number < version_min:
        logger.error('Expected Elasticsearch version range > {0} < {1}'.format(".".join(map(str,version_min)),".".join(map(str,version_max))))
        logger.error('Incompatible with version {0} of Elasticsearch.'.format(".".join(map(str,version_number))))
        sys.exit(1)

def check_master(client, master_only=False):
    """
    Check if master node.  If not, exit with error code
    """
    if master_only and not is_master_node(client):
        logger.info('Master-only flag detected. Connected to non-master node. Aborting.')
        sys.exit(9)

def get_client(**kwargs):
    """Return an Elasticsearch client using the provided parameters

    """
    kwargs['master_only'] = False if not 'master_only' in kwargs else kwargs['master_only']
    kwargs['use_ssl'] = False if not 'use_ssl' in kwargs else kwargs['use_ssl']
    kwargs['ssl_no_validate'] = False if not 'ssl_no_validate' in kwargs else kwargs['ssl_no_validate']
    kwargs['certificate'] = False if not 'certificate' in kwargs else kwargs['certificate']
    logger.debug("kwargs = {0}".format(kwargs))
    master_only = kwargs.pop('master_only')
    if kwargs['use_ssl']:
        if kwargs['ssl_no_validate']:
            kwargs['verify_certs'] = False # Not needed, but explicitly defined
        else:
            logger.info('Attempting to verify SSL certificate.')
            # If user provides a certificate:
            if kwargs['certificate']:
                kwargs['verify_certs'] = True
                kwargs['ca_certs'] = kwargs['certificate']
            else: # Try to use certifi certificates:
                try:
                    import certifi
                    kwargs['verify_certs'] = True
                    kwargs['ca_certs'] = certifi.where()
                except ImportError:
                    logger.warn('Unable to verify SSL certificate.')
    try:
        client = elasticsearch.Elasticsearch(**kwargs)
        # Verify the version is acceptable.
        check_version(client)
        # Verify "master_only" status, if applicable
        check_master(client, master_only=master_only)
        return client
    except Exception:
        logger.error('Connection failure.')
        sys.exit(1)

def override_timeout(ctx):
    """
    Override the default timeout for optimize and snapshot operations if the
    default value of 30 is provided at the command-line.
    """
    timeout = 21600
    if ctx.parent.info_name in ['optimize', 'snapshot']:
        if ctx.parent.parent.params['timeout'] == 30:
            logger.warn('Overriding default connection timeout.  New timeout: {0}'.format(timeout))
            ctx.parent.parent.params['timeout'] = timeout

def filter_callback(ctx, param, value):
    """
    Append a dict to ctx.obj['filters'] based on the arguments
    """
    # Stop here if None or empty value, but zero values are okay
    if value == 0:
        argdict = {}
    elif not value:
        return value
    else:
        argdict = {}

    if param.name in ['oldest', 'newest', 'older_than', 'newer_than']:
        if not ctx.params['time_unit'] :
            logger.error("Parameters --oldest and --newest and --older-than and --newer-than require the --time-unit parameter")
            sys.exit(1)
        if not ctx.params['timestring']:
            logger.error("Parameters --oldest and --newest and --older-than and --newer-than require the --timestring parameter")
            sys.exit(1)
        argdict = {  "groupname":'date', "time_unit":ctx.params["time_unit"],
                    "timestring": ctx.params['timestring'], "value": value,
                    "method": param.name }
        date_regex = get_date_regex(ctx.params['timestring'])
        regex = REGEX_MAP[param.name].format(date_regex)
    elif param.name == 'regex':
        regex = r'{0}'.format(value)
    elif param.name in ['prefix', 'suffix']:
        regex = REGEX_MAP[param.name].format(value)

    if param.name == 'exclude':
        for e in value:
            argdict = {}
            argdict['pattern'] = '{0}'.format(e)
            argdict['exclude'] = True
            ctx.obj['filters'].append(argdict)
            logger.debug("Added filter: {0}".format(argdict))
    else:
        logger.debug("REGEX = {0}".format(regex))
        argdict['pattern'] = regex
        ctx.obj['filters'].append(argdict)
        logger.debug("Added filter: {0}".format(argdict))
    logger.debug("New list of filters: {0}".format(ctx.obj['filters']))
    return value

def in_list(values, source_list):
    """
    Return a list of values found inside source_list.

    While a list comprehension is faster, it doesn't log failures.

    :arg values: A list of items to compare to the ``source_list``
    :arg source_list: A list of items
    """
    retval = []
    for v in values:
        if v in source_list:
            logger.info('Adding {0} from command-line argument'.format(v))
            retval.append(v)
        else:
            logger.warn('{0} not found!'.format(v))
    return retval

def do_command(client, command, indices, params=None, master_timeout=30000):
    """
    Do the command.
    """
    if command == "alias":
        return alias(
                client, indices, alias=params['name'], remove=params['remove']
               )
    if command == "allocation":
        return allocation(client, indices, rule=params['rule'], allocation_type=params['type'] )
    if command == "bloom":
        return bloom(client, indices, delay=params['delay'])
    if command == "close":
        return close(client, indices)
    if command == "delete":
        return delete(client, indices, master_timeout)
    if command == "open":
        return opener(client, indices)
    if command == "optimize":
        return optimize(
                client, indices, max_num_segments=params['max_num_segments'],
                delay=params['delay'], request_timeout=params['request_timeout']
               )
    if command == "replicas":
        return replicas(client, indices, replicas=params['count'])
    if command == "seal":
        return seal(client, indices)
    if command == "snapshot":
        return create_snapshot(
                client, indices=indices, name=params['name'],
                prefix=params['prefix'], repository=params['repository'],
                ignore_unavailable=params['ignore_unavailable'],
                include_global_state=params['include_global_state'],
                partial=params['partial'],
                wait_for_completion=params['wait_for_completion'],
                request_timeout=params['request_timeout'],
                skip_repo_validation=params['skip_repo_validation'],
               )

def msgout(msg, error=False, warning=False, quiet=False):
    """Output messages to stdout via click.echo if quiet=False"""
    if not quiet:
        if error:
            click.echo(click.style(click.style(msg, fg='red', bold=True)))
        elif warning:
            click.echo(click.style(click.style(msg, fg='yellow', bold=True)))
        else:
            click.echo(msg)
