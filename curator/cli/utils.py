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
version_max  = (2, 0, 0)
version_min = (1, 0, 0)

REGEX_MAP = {
    'timestring': r'^.*{0}.*$',
    'newer_than': r'(?P<date>{0})',
    'older_than': r'(?P<date>{0})',
    'prefix': r'^{0}.*$',
    'suffix': r'^.*{0}$',
}

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

def check_version(client):
    """
    Verify version is within acceptable range.  Exit with error if it is not.

    :arg client: The Elasticsearch client connection
    """
    version_number = get_version(client)
    logger.debug('Detected Elasticsearch version {0}'.format(".".join(map(str,version_number))))
    if version_number >= version_max or version_number < version_min:
        print('Expected Elasticsearch version range > {0} < {1}'.format(".".join(map(str,version_min)),".".join(map(str,version_max))))
        print('ERROR: Incompatible with version {0} of Elasticsearch.  Exiting.'.format(".".join(map(str,version_number))))
        sys.exit(1)

def show_vars(ctx, param, value):
    """Showing the local variables passed"""
    logger.debug("CTX! {0} has a value of {1} and the CTX contains: {2}".format(param.name, value, ctx.params))
    return value

def get_client(ctx):
    """Return an Elasticsearch client using context parameters

    """
    d = ctx.params
    logger.debug("Client CTX: {0}".format(d))

    try:
        client = elasticsearch.Elasticsearch(
            host=d["host"], http_auth=d["auth"], port=d["port"],
            url_prefix=d["url_prefix"], timeout=d["timeout"], use_ssl=d["ssl"]
            )
        # Verify the version is acceptable.
        check_version(client)
        # Verify "master_only" status, if applicable
        if d["master_only"] and not is_master_node(client):
            logger.info('Master-only flag detected. Connected to non-master node. Aborting.')
            sys.exit(9)
        return client
    except:
        print("ERROR: Connection failure.  Exiting.")
        sys.exit(1)

def validate_timestring(timestring, time_unit):
    """
    Validate that the appropriate element(s) for time_unit are in the timestring.
    e.g. If "weeks", we should see %U or %W, if hours %H, etc.

    Exit with error on failure.

    :arg timestring: An strftime string to match the datestamp in an index name.
    :arg time_unit: One of ``hours``, ``days``, ``weeks``, ``months``.  Default
        is ``days``.
    """
    fail = True
    if time_unit == 'hours':
        if '%H' in timestring:
            fail = False
    elif time_unit == 'days':
        if '%d' in timestring:
            fail = False
    elif time_unit == 'weeks':
        if '%W' in timestring:
            fail = False
        elif '%U' in timestring:
            fail = False
    elif time_unit == 'months':
        if '%m' in timestring:
            fail = False
    if fail:
        print('Timestring {0} does not match time unit {1}'.format(timestring, time_unit))
        sys.exit(1)
    return

def validate_timeout(command, timeout, timeout_override=False):
    """Validate client connection args. Correct where necessary."""
    # Override the timestamp in case the end-user doesn't.
    replacement_timeout = 21600
    # val is arbitrarily set at two hours right now.
    if command == "optimize": # This is for Elasticsearch < 1.5
        val = 7200
    elif command == "snapshot":
        val = 7200
    if timeout_override and timeout < val:
        logger.info('Timeout of {0} seconds is too low for command {1}.  Overriding to {2} seconds.'.format(timeout, command.upper(), replacement_timeout))
        timeout = replacement_timeout
    return timeout

def filter_any(param):
    """Test all flags to see if any filtering is being done"""
    retval = False
    if param.name == "regex":
        return True
    if param.name == "prefix":
        return True
    if param.name == "suffix":
        return True
    if param.name == "newer_than":
        return True
    if param.name == "older_than":
        return True
    if param.name == "timestring":
        return True
    if param.name == "exclude":
        return True
    return retval

def filter_callback(ctx, param, value):
    """
    Filter ctx.obj["filtered"] based on what shows up here.
    """
    # Stop here if None or empty value
    if not value:
        return value
    else:
        kwargs = {}

    # If we're calling a filtered object and ctx.obj['filtered'] is empty we
    # need to copy over ctx.obj['indices']
    if not ctx.obj["filtered"]:
        if filter_any(param):
            ctx.obj["filtered"] = ctx.obj["indices"]

    if param.name in ['older_than', 'newer_than']:
        kwargs = {  "groupname":'date', "time_unit":ctx.params["time_unit"],
                    "timestring": ctx.params['timestring'], "value": value,
                    "method": param.name }
        date_regex = get_date_regex(ctx.params['timestring'])
        regex = REGEX_MAP[param.name].format(date_regex)
    elif param.name == 'regex':
        regex = "r'{0}'".format(value)
    elif param.name in ['prefix', 'suffix']:
        regex = REGEX_MAP[param.name].format(value)

    if param.name == 'exclude':
        for e in value:
            logger.info('Excluding indices matching {0}'.format(e))
            pattern = re.compile(e)
            ctx.obj["filtered"] = list(filter(lambda x: not pattern.search(x), ctx.obj["filtered"]))
    else:
        logger.debug("REGEX = {0}".format(regex))
        ctx.obj["filtered"] = regex_iterate(ctx.obj["filtered"], regex, **kwargs)
    logger.debug("Filtered index list: {0}".format(ctx.obj["filtered"]))
    return value

def filter_timestring_only(ctx, timestring):
    """
    Because Click will not allow option dependencies, this is a work-around
    just in case someone is using timestamp filtering without using
    ``older_than`` or ``newer_than``
    """
    # If we're calling a filtered object and ctx.obj['filtered'] is empty we
    # need to copy over ctx.obj['indices']
    if not ctx.obj["filtered"]:
        ctx.obj["filtered"] = ctx.obj["indices"]
    date_regex = get_date_regex(timestring)
    regex = r'^.*{0}.*$'.format(date_regex)
    ctx.obj["filtered"] = regex_iterate(ctx.obj["filtered"], regex)
    logger.debug("Filtered index list: {0}".format(ctx.obj["filtered"]))

def add_indices_callback(ctx, param, value):
    """
    Add indices (if they exist) to ctx.obj["add_indices"]
    They will be added to the actionable list just before the action is executed.
    """
    # Only add an index if it actually exists, hence we check the original
    # list here
    for i in value:
        if i in ctx.obj["indices"]:
            logger.info('Adding index {0} from command-line argument'.format(i))
            ctx.obj["add_indices"].append(i)
        else:
            logger.warn('Index {0} not found!'.format(i))
    return value
