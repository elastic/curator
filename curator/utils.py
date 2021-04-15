"""Utilities"""
import time
import logging
import os
import random
import re
import string
import sys
from datetime import timedelta, datetime, date
import yaml
import elasticsearch
from voluptuous import Schema
from curator import exceptions
from curator.defaults import settings
from curator.validators import SchemaCheck, actions, filters, options
from curator._version import __version__
LOGGER = logging.getLogger(__name__)

def read_file(myfile):
    """
    Read a file and return the resulting data.

    :arg myfile: A file to read.
    :rtype: str
    """
    try:
        with open(myfile, 'r') as fhandle:
            data = fhandle.read()
        return data
    except IOError:
        raise exceptions.FailedExecution(
            'Unable to read file {0}'.format(myfile)
        )

def get_yaml(path):
    """
    Read the file identified by `path` and import its YAML contents.

    :arg path: The path to a YAML configuration file.
    :rtype: dict
    """
    # Set the stage here to parse single scalar value environment vars from
    # the YAML file being read
    single = re.compile(r'^\$\{(.*)\}$')
    yaml.add_implicit_resolver("!single", single)
    def single_constructor(loader, node):
        value = loader.construct_scalar(node)
        proto = single.match(value).group(1)
        default = None
        if len(proto.split(':')) > 1:
            envvar, default = proto.split(':')
        else:
            envvar = proto
        return os.environ[envvar] if envvar in os.environ else default

    yaml.add_constructor('!single', single_constructor)

    try:
        return yaml.load(read_file(path), Loader=yaml.FullLoader)
    except yaml.scanner.ScannerError as err:
        print('Unable to read/parse YAML file: {0}'.format(path))
        print(err)
        sys.exit(1)

def test_client_options(config):
    """
    Test whether a SSL/TLS files exist. Will raise an exception if the files
    cannot be read.

    :arg config: A client configuration file data dictionary
    :rtype: None
    """
    if config['use_ssl']:
        # Test whether certificate is a valid file path
        if 'certificate' in config and config['certificate']:
            read_file(config['certificate'])
        # Test whether client_cert is a valid file path
        if 'client_cert' in config and config['client_cert']:
            read_file(config['client_cert'])
        # Test whether client_key is a valid file path
        if 'client_key' in config and  config['client_key']:
            read_file(config['client_key'])

def rollable_alias(client, alias):
    """
    Ensure that `alias` is an alias, and points to an index that can use the
    ``_rollover`` API.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg alias: An Elasticsearch alias
    """
    try:
        response = client.indices.get_alias(name=alias)
    except elasticsearch.exceptions.NotFoundError:
        LOGGER.error('alias "{0}" not found.'.format(alias))
        return False
    # Response should be like:
    # {'there_should_be_only_one': {u'aliases': {'value of "alias" here': {}}}}
    # Where 'there_should_be_only_one' is a single index name that ends in a
    # number, and 'value of "alias" here' reflects the value of the passed
    # parameter, except in versions 6.5.0+ where the ``is_write_index`` setting
    # makes it possible to have more than one index associated with a rollover index
    if get_version(client) >= (6, 5, 0):
        for idx in response:
            if 'is_write_index' in response[idx]['aliases'][alias]:
                if response[idx]['aliases'][alias]['is_write_index']:
                    return True
    # implied `else` here: If not version 6.5.0+ and has `is_write_index`, it has to fit the
    # following criteria:
    if len(response) > 1:
        LOGGER.error(
            '"alias" must only reference one index: {0}'.format(response))
    else:
        index = list(response.keys())[0]
        rollable = False
        # In order for `rollable` to be True, the last 2 digits of the index
        # must be digits, or a hyphen followed by a digit.
        # NOTE: This is not a guarantee that the rest of the index name is
        # necessarily correctly formatted.
        if index[-2:][1].isdigit():
            if index[-2:][0].isdigit():
                rollable = True
            elif index[-2:][0] == '-':
                rollable = True
        return rollable

def verify_client_object(test):
    """
    Test if `test` is a proper :class:`elasticsearch.Elasticsearch` client
    object and raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    # Ignore mock type for testing
    if str(type(test)) == "<class 'mock.Mock'>" or \
        str(type(test)) == "<class 'mock.mock.Mock'>":
        pass
    elif not isinstance(test, elasticsearch.Elasticsearch):
        raise TypeError(
            'Not a client object. Type: {0}'.format(type(test))
        )

def verify_index_list(test):
    """
    Test if `test` is a proper :class:`curator.indexlist.IndexList` object and
    raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    # It breaks if this import isn't local to this function
    from .indexlist import IndexList
    if not isinstance(test, IndexList):
        raise TypeError(
            'Not an IndexList object. Type: {0}.'.format(type(test))
        )

def verify_snapshot_list(test):
    """
    Test if `test` is a proper :class:`curator.snapshotlist.SnapshotList`
    object and raise an exception if it is not.

    :arg test: The variable or object to test
    :rtype: None
    """
    # It breaks if this import isn't local to this function
    from .snapshotlist import SnapshotList
    if not isinstance(test, SnapshotList):
        raise TypeError(
            'Not an SnapshotList object. Type: {0}.'.format(type(test))
        )

def report_failure(exception):
    """
    Raise a `exceptions.FailedExecution` exception and include the original error message.

    :arg exception: The upstream exception.
    :rtype: None
    """
    raise exceptions.FailedExecution(
        'Exception encountered.  Rerun with loglevel DEBUG and/or check '
        'Elasticsearch logs for more information. '
        'Exception: {0}'.format(exception)
    )

def get_date_regex(timestring):
    """
    Return a regex string based on a provided strftime timestring.

    :arg timestring: An strftime pattern
    :rtype: str
    """
    prev, regex = ('', '')
    LOGGER.debug('Provided timestring = "{0}"'.format(timestring))
    for idx, char in enumerate(timestring):
        LOGGER.debug('Current character: {0} Array position: {1}'.format(char, idx))
        if char == '%':
            pass
        elif char in settings.date_regex() and prev == '%':
            regex += r'\d{' + settings.date_regex()[char] + '}'
        elif char in ['.', '-']:
            regex += "\\" + char
        else:
            regex += char
        prev = char
    LOGGER.debug("regex = {0}".format(regex))
    return regex

def get_datetime(index_timestamp, timestring):
    """
    Return the datetime extracted from the index name, which is the index
    creation time.

    :arg index_timestamp: The timestamp extracted from an index name
    :arg timestring: An strftime pattern
    :rtype: :py:class:`datetime.datetime`
    """
    # Compensate for week of year by appending '%w' to the timestring
    # and '1' (Monday) to index_timestamp
    iso_week_number = False
    if '%W' in timestring or '%U' in timestring or '%V' in timestring:
        timestring += '%w'
        index_timestamp += '1'
        if '%V' in timestring and '%G' in timestring:
            iso_week_number = True
            # Fake as so we read Greg format instead. We will process it later
            timestring = timestring.replace("%G", "%Y").replace("%V", "%W")
    elif '%m' in timestring:
        if not '%d' in timestring:
            timestring += '%d'
            index_timestamp += '1'

    date = datetime.strptime(index_timestamp, timestring)

    # Handle ISO time string
    if iso_week_number:
        date = _handle_iso_week_number(date, timestring, index_timestamp)

    return date

def fix_epoch(epoch):
    """
    Fix value of `epoch` to be epoch, which should be 10 or fewer digits long.

    :arg epoch: An epoch timestamp, in epoch + milliseconds, or microsecond, or
        even nanoseconds.
    :rtype: int
    """
    try:
        # No decimals allowed
        epoch = int(epoch)
    except Exception as err:
        raise ValueError(
            'Invalid epoch received, unable to convert {0} to int. {1}'.format(epoch, err))

    # If we're still using this script past January, 2038, we have bigger
    # problems than my hacky math here...
    if len(str(epoch)) <= 10:
        # Epoch is fine, no changes
        pass
    elif len(str(epoch)) > 10 and len(str(epoch)) <= 13:
        epoch = int(epoch/1000)
    else:
        orders_of_magnitude = len(str(epoch)) - 10
        powers_of_ten = 10**orders_of_magnitude
        epoch = int(epoch/powers_of_ten)
    return epoch

def _handle_iso_week_number(date, timestring, index_timestamp):
    date_iso = date.isocalendar()
    iso_week_str = "{Y:04d}{W:02d}".format(Y=date_iso[0], W=date_iso[1])
    greg_week_str = datetime.strftime(date, "%Y%W")

    # Edge case 1: ISO week number is bigger than Greg week number.
    # Ex: year 2014, all ISO week numbers were 1 more than in Greg.
    if (iso_week_str > greg_week_str or
            # Edge case 2: 2010-01-01 in ISO: 2009.W53, in Greg: 2010.W00
            # For Greg converting 2009.W53 gives 2010-01-04, converting back
            # to same timestring gives: 2010.W01.
            datetime.strftime(date, timestring) != index_timestamp):

        # Remove one week in this case
        date = date - timedelta(days=7)
    return date

def datetime_to_epoch(mydate):
    """Convert datetime into epoch seconds"""
    # I would have used `total_seconds`, but apparently that's new
    # to Python 2.7+, and due to so many people still using
    # RHEL/CentOS 6, I need this to support Python 2.6.
    tdelta = (mydate - datetime(1970, 1, 1))
    return tdelta.seconds + tdelta.days * 24 * 3600

class TimestringSearch(object):
    """
    An object to allow repetitive search against a string, `searchme`, without
    having to repeatedly recreate the regex.

    :arg timestring: An strftime pattern
    """
    def __init__(self, timestring):
        regex = r'(?P<date>{0})'.format(get_date_regex(timestring))
        self.pattern = re.compile(regex)
        self.timestring = timestring
    def get_epoch(self, searchme):
        """
        Return the epoch timestamp extracted from the `timestring` appearing in
        `searchme`.

        :arg searchme: A string to be searched for a date pattern that matches
            `timestring`
        :rtype: int
        """
        match = self.pattern.search(searchme)
        if match:
            if match.group("date"):
                timestamp = match.group("date")
                return datetime_to_epoch(
                    get_datetime(timestamp, self.timestring)
                )
                # # I would have used `total_seconds`, but apparently that's new
                # # to Python 2.7+, and due to so many people still using
                # # RHEL/CentOS 6, I need this to support Python 2.6.
                # tdelta = (
                #     get_datetime(timestamp, self.timestring) -
                #     datetime(1970,1,1)
                # )
                # return tdelta.seconds + tdelta.days * 24 * 3600

def get_point_of_reference(unit, count, epoch=None):
    """
    Get a point-of-reference timestamp in epoch + milliseconds by deriving
    from a `unit` and a `count`, and an optional reference timestamp, `epoch`

    :arg unit: One of ``seconds``, ``minutes``, ``hours``, ``days``, ``weeks``,
        ``months``, or ``years``.
    :arg unit_count: The number of ``units``. ``unit_count`` * ``unit`` will
        be calculated out to the relative number of seconds.
    :arg epoch: An epoch timestamp used in conjunction with ``unit`` and
        ``unit_count`` to establish a point of reference for calculations.
    :rtype: int
    """
    if unit == 'seconds':
        multiplier = 1
    elif unit == 'minutes':
        multiplier = 60
    elif unit == 'hours':
        multiplier = 3600
    elif unit == 'days':
        multiplier = 3600*24
    elif unit == 'weeks':
        multiplier = 3600*24*7
    elif unit == 'months':
        multiplier = 3600*24*30
    elif unit == 'years':
        multiplier = 3600*24*365
    else:
        raise ValueError('Invalid unit: {0}.'.format(unit))
    # Use this moment as a reference point, if one is not provided.
    if not epoch:
        epoch = time.time()
    epoch = fix_epoch(epoch)
    return epoch - multiplier * count

def get_unit_count_from_name(index_name, pattern):
    """Derive the unit_count from the index name"""
    if pattern is None:
        return None
    match = pattern.search(index_name)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return None
    else:
        return None

def date_range(unit, range_from, range_to, epoch=None, week_starts_on='sunday'):
    """
    Get the epoch start time and end time of a range of ``unit``s, reckoning the
    start of the week (if that's the selected unit) based on ``week_starts_on``,
    which can be either ``sunday`` or ``monday``.

    :arg unit: One of ``hours``, ``days``, ``weeks``, ``months``, or ``years``.
    :arg range_from: How many ``unit`` (s) in the past/future is the origin?
    :arg range_to: How many ``unit`` (s) in the past/future is the end point?
    :arg epoch: An epoch timestamp used to establish a point of reference for
        calculations.
    :arg week_starts_on: Either ``sunday`` or ``monday``. Default is ``sunday``
    :rtype: tuple
    """
    acceptable_units = ['hours', 'days', 'weeks', 'months', 'years']
    if unit not in acceptable_units:
        raise exceptions.ConfigurationError(
            '"unit" must be one of: {0}'.format(acceptable_units))
    if not range_to >= range_from:
        raise exceptions.ConfigurationError(
            '"range_to" must be greater than or equal to "range_from"')
    if not epoch:
        epoch = time.time()
    epoch = fix_epoch(epoch)
    raw_point_of_ref = datetime.utcfromtimestamp(epoch)
    LOGGER.debug('Raw point of Reference = {0}'.format(raw_point_of_ref))
    # Reverse the polarity, because -1 as last week makes sense when read by
    # humans, but datetime timedelta math makes -1 in the future.
    origin = range_from * -1
    # These if statements help get the start date or start_delta
    if unit == 'hours':
        point_of_ref = datetime(
            raw_point_of_ref.year, raw_point_of_ref.month, raw_point_of_ref.day,
            raw_point_of_ref.hour, 0, 0
        )
        start_delta = timedelta(hours=origin)
    if unit == 'days':
        point_of_ref = datetime(
            raw_point_of_ref.year, raw_point_of_ref.month,
            raw_point_of_ref.day, 0, 0, 0
            )
        start_delta = timedelta(days=origin)
    if unit == 'weeks':
        point_of_ref = datetime(
            raw_point_of_ref.year, raw_point_of_ref.month, raw_point_of_ref.day, 0, 0, 0)
        sunday = False
        if week_starts_on.lower() == 'sunday':
            sunday = True
        weekday = point_of_ref.weekday()
        # Compensate for ISO week starting on Monday by default
        if sunday:
            weekday += 1
        LOGGER.debug('Weekday = {0}'.format(weekday))
        start_delta = timedelta(days=weekday, weeks=origin)
    if unit == 'months':
        point_of_ref = datetime(raw_point_of_ref.year, raw_point_of_ref.month, 1, 0, 0, 0)
        year = raw_point_of_ref.year
        month = raw_point_of_ref.month
        if origin > 0:
            for _ in range(0, origin):
                if month == 1:
                    year -= 1
                    month = 12
                else:
                    month -= 1
        else:
            for _ in range(origin, 0):
                if month == 12:
                    year += 1
                    month = 1
                else:
                    month += 1
        start_date = datetime(year, month, 1, 0, 0, 0)
    if unit == 'years':
        point_of_ref = datetime(raw_point_of_ref.year, 1, 1, 0, 0, 0)
        start_date = datetime(raw_point_of_ref.year - origin, 1, 1, 0, 0, 0)
    if unit not in ['months', 'years']:
        start_date = point_of_ref - start_delta
    # By this point, we know our start date and can convert it to epoch time
    start_epoch = datetime_to_epoch(start_date)
    LOGGER.debug('Start ISO8601 = {0}'.format(datetime.utcfromtimestamp(start_epoch).isoformat()))
    # This is the number of units we need to consider.
    count = (range_to - range_from) + 1
    # We have to iterate to one more month, and then subtract a second to get
    # the last day of the correct month
    if unit == 'months':
        month = start_date.month
        year = start_date.year
        for _ in range(0, count):
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
        end_date = datetime(year, month, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(end_date) - 1
    # Similarly, with years, we need to get the last moment of the year
    elif unit == 'years':
        end_date = datetime((raw_point_of_ref.year - origin) + count, 1, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(end_date) - 1
    # It's not months or years, which have inconsistent reckoning...
    else:
        # This lets us use an existing method to simply add unit * count seconds
        # to get hours, days, or weeks, as they don't change
        end_epoch = get_point_of_reference(
            unit, count * -1, epoch=start_epoch) -1
    LOGGER.debug('End ISO8601 = {0}'.format(
        datetime.utcfromtimestamp(end_epoch).isoformat()))
    return (start_epoch, end_epoch)

def absolute_date_range(
        unit, date_from, date_to,
        date_from_format=None, date_to_format=None
    ):
    """
    Get the epoch start time and end time of a range of ``unit``s, reckoning the
    start of the week (if that's the selected unit) based on ``week_starts_on``,
    which can be either ``sunday`` or ``monday``.

    :arg unit: One of ``hours``, ``days``, ``weeks``, ``months``, or ``years``.
    :arg date_from: The simplified date for the start of the range
    :arg date_to: The simplified date for the end of the range.  If this value
        is the same as ``date_from``, the full value of ``unit`` will be
        extrapolated for the range.  For example, if ``unit`` is ``months``,
        and ``date_from`` and ``date_to`` are both ``2017.01``, then the entire
        month of January 2017 will be the absolute date range.
    :arg date_from_format: The strftime string used to parse ``date_from``
    :arg date_to_format: The strftime string used to parse ``date_to``
    :rtype: tuple
    """
    acceptable_units = ['seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years']
    if unit not in acceptable_units:
        raise exceptions.ConfigurationError(
            '"unit" must be one of: {0}'.format(acceptable_units))
    if not date_from_format or not date_to_format:
        raise exceptions.ConfigurationError('Must provide "date_from_format" and "date_to_format"')
    try:
        start_epoch = datetime_to_epoch(get_datetime(date_from, date_from_format))
        LOGGER.debug(
            'Start ISO8601 = {0}'.format(datetime.utcfromtimestamp(start_epoch).isoformat()))
    except Exception as err:
        raise exceptions.ConfigurationError(
            'Unable to parse "date_from" {0} and "date_from_format" {1}. '
            'Error: {2}'.format(date_from, date_from_format, err)
        )
    try:
        end_date = get_datetime(date_to, date_to_format)
    except Exception as err:
        raise exceptions.ConfigurationError(
            'Unable to parse "date_to" {0} and "date_to_format" {1}. '
            'Error: {2}'.format(date_to, date_to_format, err)
        )
    # We have to iterate to one more month, and then subtract a second to get
    # the last day of the correct month
    if unit == 'months':
        month = end_date.month
        year = end_date.year
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        new_end_date = datetime(year, month, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(new_end_date) - 1
    # Similarly, with years, we need to get the last moment of the year
    elif unit == 'years':
        new_end_date = datetime(end_date.year + 1, 1, 1, 0, 0, 0)
        end_epoch = datetime_to_epoch(new_end_date) - 1
    # It's not months or years, which have inconsistent reckoning...
    else:
        # This lets us use an existing method to simply add 1 more unit's worth
        # of seconds to get hours, days, or weeks, as they don't change
        # We use -1 as point of reference normally subtracts from the epoch
        # and we need to add to it, so we'll make it subtract a negative value.
        # Then, as before, subtract 1 to get the end of the period
        end_epoch = get_point_of_reference(
            unit, -1, epoch=datetime_to_epoch(end_date)) -1

    LOGGER.debug('End ISO8601 = {0}'.format(
        datetime.utcfromtimestamp(end_epoch).isoformat()))
    return (start_epoch, end_epoch)

def byte_size(num, suffix='B'):
    """
    Return a formatted string indicating the size in bytes, with the proper
    unit, e.g. KB, MB, GB, TB, etc.

    :arg num: The number of byte
    :arg suffix: An arbitrary suffix, like `Bytes`
    :rtype: float
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)

def ensure_list(indices):
    """
    Return a list, even if indices is a single value

    :arg indices: A list of indices to act upon
    :rtype: list
    """
    if not isinstance(indices, list): # in case of a single value passed
        indices = [indices]
    return indices

def to_csv(indices):
    """
    Return a csv string from a list of indices, or a single value if only one
    value is present

    :arg indices: A list of indices to act on, or a single value, which could be
        in the format of a csv string already.
    :rtype: str
    """
    indices = ensure_list(indices) # in case of a single value passed
    if indices:
        return ','.join(sorted(indices))
    else:
        return None

def check_csv(value):
    """
    Some of the curator methods should not operate against multiple indices at
    once.  This method can be used to check if a list or csv has been sent.

    :arg value: The value to test, if list or csv string
    :rtype: bool
    """
    if isinstance(value, list):
        return True
    # Python3 hack because it doesn't recognize unicode as a type anymore
    if sys.version_info < (3, 0):
        # pylint: disable=E0602
        if isinstance(value, unicode):
            value = str(value)
    if isinstance(value, str):
        if len(value.split(',')) > 1: # It's a csv string.
            return True
        else: # There's only one value here, so it's not a csv string
            return False
    else:
        raise TypeError(
            'Passed value: {0} is not a list or a string '
            'but is of type {1}'.format(value, type(value))
        )

def chunk_index_list(indices):
    """
    This utility chunks very large index lists into 3KB chunks
    It measures the size as a csv string, then converts back into a list
    for the return value.

    :arg indices: A list of indices to act on.
    :rtype: list
    """
    chunks = []
    chunk = ""
    for index in indices:
        if len(chunk) < 3072:
            if not chunk:
                chunk = index
            else:
                chunk += "," + index
        else:
            chunks.append(chunk.split(','))
            chunk = index
    chunks.append(chunk.split(','))
    return chunks

def get_indices(client):
    """
    Get the current list of indices from the cluster.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: list
    """
    try:
        indices = list(
            client.indices.get_settings(index='_all', params={'expand_wildcards': 'open,closed'})
        )
        version_number = get_version(client)
        LOGGER.debug(
            'Detected Elasticsearch version '
            '{0}'.format(".".join(map(str, version_number)))
        )
        LOGGER.debug("All indices: {0}".format(indices))
        return indices
    except Exception as err:
        raise exceptions.FailedExecution('Failed to get indices. Error: {0}'.format(err))

def get_version(client):
    """
    Return the ES version number as a tuple.
    Omits trailing tags like -dev, or Beta

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: tuple
    """
    version = client.info()['version']['number']
    version = version.split('-')[0]
    if len(version.split('.')) > 3:
        version = version.split('.')[:-1]
    else:
       version = version.split('.')
    return tuple(map(int, version))

def is_master_node(client):
    """
    Return `True` if the connected client node is the elected master node in
    the Elasticsearch cluster, otherwise return `False`.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    my_node_id = list(client.nodes.info('_local')['nodes'])[0]
    master_node_id = client.cluster.state(metric='master_node')['master_node']
    return my_node_id == master_node_id

def check_version(client):
    """
    Verify version is within acceptable range.  Raise an exception if it is not.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: None
    """
    version_number = get_version(client)
    LOGGER.debug('Detected Elasticsearch version {0}'.format(".".join(map(str, version_number))))
    if version_number >= settings.version_max() \
        or version_number < settings.version_min():
        LOGGER.error(
            'Elasticsearch version {0} incompatible with this version of Curator '
            '({1})'.format(".".join(map(str, version_number)), __version__)
        )
        raise exceptions.CuratorException(
            'Elasticsearch version {0} incompatible with this version of Curator '
            '({1})'.format(".".join(map(str, version_number)), __version__)
        )

def check_master(client, master_only=False):
    """
    Check if connected client is the elected master node of the cluster.
    If not, cleanly exit with a log message.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: None
    """
    if master_only and not is_master_node(client):
        LOGGER.info(
            'Master-only flag detected. '
            'Connected to non-master node. Aborting.'
        )
        sys.exit(0)

def process_url_prefix_arg(data):
    """Test for and validate the ``url_prefix`` setting"""
    if 'url_prefix' in data:
        if (data['url_prefix'] is None or data['url_prefix'] == "None"):
            data['url_prefix'] = ''
    return data

def process_host_args(data):
    """
    Check for ``host`` and ``hosts`` in the provided dictionary.
    Raise an exception if both ``host`` and ``hosts`` are present.
    If ``host`` is used, replace that with ``hosts``.
    """
    if 'host' in data and 'hosts' in data:
        raise exceptions.ConfigurationError(
            'Both "host" and "hosts" are defined.  Pick only one.')
    elif 'host' in data and 'hosts' not in data:
        data['hosts'] = data['host']
        del data['host']
    data['hosts'] = '127.0.0.1' if 'hosts' not in data else data['hosts']
    data['hosts'] = ensure_list(data['hosts'])
    return data

def process_x_api_key_arg(data):
    """Test for arg and set x-api-key header if present"""
    api_key = data.pop('api_key', False)
    if api_key:
        data['headers'] = {'x-api-key': api_key}
    return data

def process_master_only_arg(data):
    """
    Test whether there are multiple hosts and ``master_only`` is ``True``

    Return the data/kwargs minus the ``master_only`` key/value pair if the test passes. Otherwise,
    raise an exception.
    """
    master_only = data.pop('master_only', False)
    if master_only:
        if len(data['hosts']) > 1:
            LOGGER.error(
                '"master_only" cannot be true if more than one host is '
                'specified. Hosts = {0}'.format(data['hosts'])
            )
            raise exceptions.ConfigurationError(
                '"master_only" cannot be true if more than one host is '
                'specified. Hosts = {0}'.format(data['hosts'])
            )
    return data, master_only

def process_auth_args(data):
    """
    Return a valid http_auth tuple for authentication in the elasticsearch.Elasticsearch
    client object
    """
    http_auth = data['http_auth'] if 'http_auth' in data else None
    username = data.pop('username', False)
    password = data.pop('password', False)
    if http_auth:
        # No change to http_auth
        LOGGER.warn(
            'Use of "http_auth" is deprecated. Please use "username" and "password" instead.')
    elif username and password:
        http_auth = (username, password)
    elif not username and password:
        LOGGER.error('Password provided without username.')
        LOGGER.fatal('Curator cannot proceed. Exiting.')
        raise exceptions.ClientException
    elif username and not password:
        LOGGER.error('Username provided without password.')
        LOGGER.fatal('Curator cannot proceed. Exiting.')
        raise exceptions.ClientException
    # else all are empty or None, so no worries. Return as-is
    data['http_auth'] = http_auth
    return data

def process_ssl_args(data):
    """Populate and validate the proper SSL args in data and return it"""
    data['use_ssl'] = False if 'use_ssl' not in data else data['use_ssl']
    data['ssl_no_validate'] = False if 'ssl_no_validate' not in data \
        else data['ssl_no_validate']
    data['certificate'] = False if 'certificate' not in data \
        else data['certificate']
    data['client_cert'] = False if 'client_cert' not in data \
        else data['client_cert']
    data['client_key'] = False if 'client_key' not in data \
        else data['client_key']
    if data['use_ssl']:
        if data['ssl_no_validate']:
            data['verify_certs'] = False # Not needed, but explicitly defined
        else:
            LOGGER.debug('Attempting to verify SSL certificate.')
            # If user provides a certificate:
            if data['certificate']:
                data['verify_certs'] = True
                data['ca_certs'] = data['certificate']
            else: # Try to use bundled certifi certificates
                if getattr(sys, 'frozen', False):
                    # The application is frozen (compiled)
                    datadir = os.path.dirname(sys.executable)
                    data['verify_certs'] = True
                    data['ca_certs'] = os.path.join(datadir, 'cacert.pem')
                else:
                    # Use certifi certificates via certifi.where():
                    import certifi
                    data['verify_certs'] = True
                    data['ca_certs'] = certifi.where()
    return data

def process_aws_args(data):
    """Process all AWS client args. Raise exceptions if they are incomplete"""
    data['aws_key'] = False if 'aws_key' not in data else data['aws_key']
    data['aws_secret_key'] = False if 'aws_secret_key' not in data else data['aws_secret_key']
    data['aws_token'] = '' if 'aws_token' not in data else data['aws_token']
    data['aws_sign_request'] = False if 'aws_sign_request' not in data \
        else data['aws_sign_request']
    data['aws_region'] = False if 'aws_region' not in data \
        else data['aws_region']
    if data['aws_key'] or data['aws_secret_key'] or data['aws_sign_request']:
        if not data['aws_region']:
            raise exceptions.MissingArgument('Missing "aws_region".')
        if data['aws_key'] or data['aws_secret_key']:
            if not (data['aws_key'] and data['aws_secret_key']):
                raise exceptions.MissingArgument('Missing AWS Access Key or AWS Secret Key')
    return data

def try_boto_session(data):
    """Try to obtain AWS credentials using boto"""
    if data['aws_sign_request']:
        try:
            from boto3 import session
            from botocore import exceptions as botoex
        # We cannot get credentials without the boto3 library, so we cannot continue
        except ImportError as err:
            LOGGER.error('Unable to sign AWS requests. Failed to import a module: {0}'.format(err))
            raise ImportError('Failed to import a module: {0}'.format(err))
        try:
            if 'aws_region' in data:
                session = session.Session(region_name=data['aws_region'])
            else:
                session = session.Session()
            credentials = session.get_credentials()
            data['aws_key'] = credentials.access_key
            data['aws_secret_key'] = credentials.secret_key
            data['aws_token'] = credentials.token
        # If an attribute doesn't exist, we were not able to retrieve credentials
        # as expected so we can't continue
        except AttributeError:
            LOGGER.debug('Unable to locate AWS credentials')
            raise botoex.NoCredentialsError
    return data

def try_aws_auth(data):
    """Set ``data`` with AWS credentials and the requisite SSL flags if detected"""
    LOGGER.debug('Checking for AWS settings')
    has_requests_module = False
    try:
        from requests_aws4auth import AWS4Auth
        has_requests_module = True
    except ImportError:
        LOGGER.debug('Not using "requests_aws4auth" python module to connect.')
    if has_requests_module:
        if data['aws_key']:
            LOGGER.info('Configuring client to connect to AWS endpoint')
            # Override these key values
            data['use_ssl'] = True
            data['verify_certs'] = True
            if data['ssl_no_validate']:
                data['verify_certs'] = False
            data['http_auth'] = (
                AWS4Auth(
                    data['aws_key'], data['aws_secret_key'],
                    data['aws_region'], 'es', session_token=data['aws_token'])
            )
    return data

def do_version_check(client, skip):
    """
    Do a test of the Elasticsearch version, unless ``skip`` is ``True``
    """
    if skip:
        LOGGER.warn(
            'Skipping Elasticsearch version verification. This is '
            'acceptable for remote reindex operations.'
        )
    else:
        LOGGER.debug('Checking Elasticsearch endpoint version...')
        try:
            # Verify the version is acceptable.
            check_version(client)
        except exceptions.CuratorException as err:
            LOGGER.error('{0}'.format(err))
            LOGGER.fatal('Curator cannot continue due to version incompatibilites. Exiting')
            raise exceptions.ClientException

def verify_master_status(client, master_only):
    """
    Verify that the client is connected to the elected master node.
    Raise an exception if it is not.
    """
    # Verify "master_only" status, if applicable
    if master_only:
        LOGGER.info('Connecting only to local master node...')
        try:
            check_master(client, master_only=master_only)
        except exceptions.ConfigurationError as err:
            LOGGER.error('master_only check failed: {0}'.format(err))
            LOGGER.fatal('Curator cannot continue. Exiting.')
            raise exceptions.ClientException
    else:
        LOGGER.debug('Not verifying local master status (master_only: false)')


def get_client(**kwargs):
    """
    NOTE: AWS IAM parameters `aws_sign_request` and `aws_region` are
     provided to facilitate request signing. The credentials will be
     fetched from the local environment as per the AWS documentation:
     http://amzn.to/2fRCGCt

    AWS IAM parameters `aws_key`, `aws_secret_key`, and `aws_region` are
    provided for users that still have their keys included in the Curator config file.

    Return an :class:`elasticsearch.Elasticsearch` client object using the
    provided parameters. Any of the keyword arguments the
    :class:`elasticsearch.Elasticsearch` client object can receive are valid,
    such as:

    :arg hosts: A list of one or more Elasticsearch client hostnames or IP
        addresses to connect to.  Can send a single host.
    :type hosts: list
    :arg port: The Elasticsearch client port to connect to.
    :type port: int
    :arg url_prefix: `Optional` url prefix, if needed to reach the Elasticsearch
        API (i.e., it's not at the root level)
    :type url_prefix: str
    :arg use_ssl: Whether to connect to the client via SSL/TLS
    :type use_ssl: bool
    :arg certificate: Path to SSL/TLS certificate
    :arg client_cert: Path to SSL/TLS client certificate (public key)
    :arg client_key: Path to SSL/TLS private key
    :arg aws_key: AWS IAM Access Key (Only used if the :mod:`requests-aws4auth`
        python module is installed)
    :arg aws_secret_key: AWS IAM Secret Access Key (Only used if the
        :mod:`requests-aws4auth` python module is installed)
    :arg aws_region: AWS Region (Only used if the :mod:`requests-aws4auth`
        python module is installed)
    :arg aws_sign_request: Sign request to AWS (Only used if the :mod:`requests-aws4auth`
         and :mod:`boto3` python modules are installed)
     :arg aws_region: AWS Region where the cluster exists (Only used if the :mod:`requests-aws4auth`
         and :mod:`boto3` python modules are installed)
    :arg ssl_no_validate: If `True`, do not validate the certificate
        chain.  This is an insecure option and you will see warnings in the
        log output.
    :type ssl_no_validate: bool
    :arg username: HTTP basic authentication username. Ignored if ``http_auth`` is set.
    :type username: str
    :arg password: HTTP basic authentication password. Ignored if ``http_auth`` is set.
    :type password: str
    :arg http_auth: Authentication credentials in `user:pass` format.
    :type http_auth: str
    :arg timeout: Number of seconds before the client will timeout.
    :type timeout: int
    :arg master_only: If `True`, the client will `only` connect if the
        endpoint is the elected master node of the cluster.  **This option does
        not work if `hosts` has more than one value.**  It will raise an
        Exception in that case.
    :type master_only: bool
    :arg skip_version_test: If `True`, skip the version check as part of the
        client connection.
    :rtype: :class:`elasticsearch.Elasticsearch`
    :arg api_key: value to be used in optional X-Api-key header when accessing Elasticsearch
    :type api_key: str
    """
    # Walk through parsing/testing series of arguments to build the client
    skip_version_test = kwargs.pop('skip_version_test', False)
    kwargs = process_url_prefix_arg(kwargs)
    kwargs = process_host_args(kwargs)
    kwargs = process_x_api_key_arg(kwargs)
    kwargs['connection_class'] = elasticsearch.RequestsHttpConnection
    kwargs = process_ssl_args(kwargs)
    kwargs = process_aws_args(kwargs)
    kwargs = try_boto_session(kwargs)
    kwargs = try_aws_auth(kwargs)
    kwargs, master_only = process_master_only_arg(kwargs)
    kwargs = process_auth_args(kwargs)

    LOGGER.debug("kwargs = {0}".format(kwargs))
    fail = False

    try:
        # Creating the class object should be okay
        LOGGER.info('Instantiating client object')
        client = elasticsearch.Elasticsearch(**kwargs)
        # Test client connectivity (debug log client.info() output)
        LOGGER.info('Testing client connectivity')
        LOGGER.debug('Cluster info: {0}'.format(client.info()))
        LOGGER.info('Successfully created Elasticsearch client object with provided settings')
    # Catch all TransportError types first
    except elasticsearch.TransportError as err:
        try:
            reason = err.info['error']['reason']
        except:
            reason = err.error
        LOGGER.error('HTTP {0} error: {1}'.format(err.status_code, reason))
        fail = True
    # Catch other potential exceptions
    except Exception as err:
        LOGGER.error('Unable to connect to Elasticsearch cluster. Error: {0}'.format(err))
        fail = True
    ## failure checks
    # First level failure check
    if fail:
        LOGGER.fatal('Curator cannot proceed. Exiting.')
        raise exceptions.ClientException
    # Second level failure check: acceptable version
    do_version_check(client, skip_version_test)
    # Third level failure check: master_only
    verify_master_status(client, master_only)
    return client

def show_dry_run(ilo, action, **kwargs):
    """
    Log dry run output with the action which would have been executed.

    :arg ilo: A :class:`curator.indexlist.IndexList`
    :arg action: The `action` to be performed.
    :arg kwargs: Any other args to show in the log output
    """
    LOGGER.info('DRY-RUN MODE.  No changes will be made.')
    LOGGER.info(
        '(CLOSED) indices may be shown that may not be acted on by action "{0}".'.format(action)
    )
    indices = sorted(ilo.indices)
    for idx in indices:
            index_closed = ilo.index_info[idx]['state'] == 'close'
            LOGGER.info(
                'DRY-RUN: {0}: {1}{2} with arguments: '
                '{3}'.format(action, idx, ' (CLOSED)' if index_closed else '', kwargs)
            )

### SNAPSHOT STUFF ###
def get_repository(client, repository=''):
    """
    Return configuration information for the indicated repository.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: dict
    """
    try:
        return client.snapshot.get_repository(repository=repository)
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as err:
        raise exceptions.CuratorException(
            'Unable to get repository {0}.  Response Code: {1}  Error: {2} Check Elasticsearch '
            'logs for more information.'.format(repository, err.status_code, err.error)
        )

def get_snapshot(client, repository=None, snapshot=''):
    """
    Return information about a snapshot (or a comma-separated list of snapshots)
    If no snapshot specified, it will return all snapshots.  If none exist, an
    empty dictionary will be returned.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name, or a comma-separated list of snapshots
    :rtype: dict
    """
    if not repository:
        raise exceptions.MissingArgument('No value for "repository" provided')
    snapname = '_all' if snapshot == '' else snapshot
    try:
        return client.snapshot.get(repository=repository, snapshot=snapshot)
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as err:
        raise exceptions.FailedExecution(
            'Unable to get information about snapshot {0} from repository: '
            '{1}.  Error: {2}'.format(snapname, repository, err)
        )

def get_snapshot_data(client, repository=None):
    """
    Get ``_all`` snapshots from repository and return a list.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: list
    """
    if not repository:
        raise exceptions.MissingArgument('No value for "repository" provided')
    try:
        return client.snapshot.get(repository=repository, snapshot="_all")['snapshots']
    except (elasticsearch.TransportError, elasticsearch.NotFoundError) as err:
        raise exceptions.FailedExecution(
            'Unable to get snapshot information from repository: '
            '{0}. Error: {1}'.format(repository, err)
        )

def snapshot_in_progress(client, repository=None, snapshot=None):
    """
    Determine whether the provided snapshot in `repository` is ``IN_PROGRESS``.
    If no value is provided for `snapshot`, then check all of them.
    Return `snapshot` if it is found to be in progress, or `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name
    """
    allsnaps = get_snapshot_data(client, repository=repository)
    inprogress = (
        [snap['snapshot'] for snap in allsnaps if 'state' in snap.keys() \
            and snap['state'] == 'IN_PROGRESS']
    )
    if snapshot:
        retval = snapshot if snapshot in inprogress else False
    else:
        if not inprogress:
            retval = False
        elif len(inprogress) == 1:
            retval = inprogress[0]
        else: # This should not be possible
            raise exceptions.CuratorException(
                'More than 1 snapshot in progress: {0}'.format(inprogress)
            )
    return retval

def find_snapshot_tasks(client):
    """
    Check if there is snapshot activity in the Tasks API.
    Return `True` if activity is found, or `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    retval = False
    tasklist = client.tasks.list()
    for node in tasklist['nodes']:
        for task in tasklist['nodes'][node]['tasks']:
            activity = tasklist['nodes'][node]['tasks'][task]['action']
            if 'snapshot' in activity:
                LOGGER.debug('Snapshot activity detected: {0}'.format(activity))
                retval = True
    return retval

def safe_to_snap(client, repository=None, retry_interval=120, retry_count=3):
    """
    Ensure there are no snapshots in progress.  Pause and retry accordingly

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg retry_interval: Number of seconds to delay betwen retries. Default:
        120 (seconds)
    :arg retry_count: Number of attempts to make. Default: 3
    :rtype: bool
    """
    if not repository:
        raise exceptions.MissingArgument('No value for "repository" provided')
    for count in range(1, retry_count+1):
        in_progress = snapshot_in_progress(
            client, repository=repository
        )
        ongoing_task = find_snapshot_tasks(client)
        if in_progress or ongoing_task:
            if in_progress:
                LOGGER.info(
                    'Snapshot already in progress: {0}'.format(in_progress))
            elif ongoing_task:
                LOGGER.info('Snapshot activity detected in Tasks API')
            LOGGER.info(
                'Pausing {0} seconds before retrying...'.format(retry_interval))
            time.sleep(retry_interval)
            LOGGER.info('Retry {0} of {1}'.format(count, retry_count))
        else:
            return True
    return False

def create_snapshot_body(indices, ignore_unavailable=False,
                         include_global_state=True, partial=False):
    """
    Create the request body for creating a snapshot from the provided
    arguments.

    :arg indices: A single index, or list of indices to snapshot.
    :arg ignore_unavailable: Ignore unavailable shards/indices. (default:
        `False`)
    :type ignore_unavailable: bool
    :arg include_global_state: Store cluster global state with snapshot.
        (default: `True`)
    :type include_global_state: bool
    :arg partial: Do not fail if primary shard is unavailable. (default:
        `False`)
    :type partial: bool
    :rtype: dict
    """
    if not indices:
        LOGGER.error('No indices provided.')
        return False
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if indices == '_all':
        body["indices"] = indices
    else:
        body["indices"] = to_csv(indices)
    return body

def create_repo_body(repo_type=None,
                     compress=True, chunk_size=None,
                     max_restore_bytes_per_sec=None,
                     max_snapshot_bytes_per_sec=None,
                     location=None,
                     bucket=None, region=None, base_path=None, access_key=None,
                     secret_key=None,
                     role_arn=None, **kwargs):
    """
    Build the 'body' portion for use in creating a repository.

    :arg repo_type: The type of repository (presently only `fs` and `s3`)
    :arg compress: Turn on compression of the snapshot files. Compression is
        applied only to metadata files (index mapping and settings). Data files
        are not compressed. (Default: `True`)
    :arg chunk_size: The chunk size can be specified in bytes or by using size
        value notation, i.e. 1g, 10m, 5k. Defaults to `null` (unlimited chunk
        size).
    :arg max_restore_bytes_per_sec: Throttles per node restore rate. Defaults
        to ``20mb`` per second.
    :arg max_snapshot_bytes_per_sec: Throttles per node snapshot rate. Defaults
        to ``20mb`` per second.
    :arg location: Location of the snapshots. Required.
    :arg bucket: `S3 only.` The name of the bucket to be used for snapshots.
        Required.
    :arg region: `S3 only.` The region where bucket is located. Defaults to
        `US Standard`
    :arg base_path: `S3 only.` Specifies the path within bucket to repository
        data. Defaults to value of ``repositories.s3.base_path`` or to root
        directory if not set.
    :arg access_key: `S3 only.` The access key to use for authentication.
        Defaults to value of ``cloud.aws.access_key``.
    :arg secret_key: `S3 only.` The secret key to use for authentication.
        Defaults to value of ``cloud.aws.secret_key``.
    :arg role_arn: `S3 only.` The role arn for snapshot registration.
        Required.

    :returns: A dictionary suitable for creating a repository from the provided
        arguments.
    :rtype: dict
    """
    # This shouldn't happen, but just in case...
    if not repo_type:
        raise exceptions.MissingArgument('Missing required parameter --repo_type')

    argdict = locals()
    body = {}
    body['type'] = argdict['repo_type']
    body['settings'] = {}
    settingz = [] # Differentiate from module settings
    maybes = [
        'compress', 'chunk_size', 'max_restore_bytes_per_sec', 'max_snapshot_bytes_per_sec']
    s3args = ['bucket', 'region', 'base_path', 'access_key', 'secret_key', 'role_arn']

    settingz += [i for i in maybes if argdict[i]]
    # Type 'fs'
    if argdict['repo_type'] == 'fs':
        settingz.append('location')
    # Type 's3'
    if argdict['repo_type'] == 's3':
        settingz += [i for i in s3args if argdict[i]]
    for k in settingz:
        body['settings'][k] = argdict[k]
    return body

def create_repository(client, **kwargs):
    """
    Create repository with repository and body settings

    :arg client: An :class:`elasticsearch.Elasticsearch` client object

    :arg repository: The Elasticsearch snapshot repository to use
    :arg repo_type: The type of repository (presently only `fs` and `s3`)
    :arg compress: Turn on compression of the snapshot files. Compression is
        applied only to metadata files (index mapping and settings). Data files
        are not compressed. (Default: `True`)
    :arg chunk_size: The chunk size can be specified in bytes or by using size
        value notation, i.e. 1g, 10m, 5k. Defaults to `null` (unlimited chunk
        size).
    :arg max_restore_bytes_per_sec: Throttles per node restore rate. Defaults
        to ``20mb`` per second.
    :arg max_snapshot_bytes_per_sec: Throttles per node snapshot rate. Defaults
        to ``20mb`` per second.
    :arg location: Location of the snapshots. Required.
    :arg bucket: `S3 only.` The name of the bucket to be used for snapshots.
        Required.
    :arg region: `S3 only.` The region where bucket is located. Defaults to
        `US Standard`
    :arg base_path: `S3 only.` Specifies the path within bucket to repository
        data. Defaults to value of ``repositories.s3.base_path`` or to root
        directory if not set.
    :arg access_key: `S3 only.` The access key to use for authentication.
        Defaults to value of ``cloud.aws.access_key``.
    :arg secret_key: `S3 only.` The secret key to use for authentication.
        Defaults to value of ``cloud.aws.secret_key``.
    :arg skip_repo_fs_check: Skip verifying the repo after creation.

    :returns: A boolean value indicating success or failure.
    :rtype: bool
    """
    if 'repository' not in kwargs:
        raise exceptions.MissingArgument('Missing required parameter "repository"')
    else:
        repository = kwargs['repository']
    skip_repo_fs_check = kwargs.pop('skip_repo_fs_check', False)
    params = {'verify': 'false' if skip_repo_fs_check else 'true'}

    try:
        body = create_repo_body(**kwargs)
        LOGGER.debug('Checking if repository {0} already exists...'.format(repository))
        result = repository_exists(client, repository=repository)
        LOGGER.debug('Result = {0}'.format(result))
        if not result:
            LOGGER.debug('Repository {0} not in Elasticsearch. Continuing...'.format(repository))
            client.snapshot.create_repository(repository=repository, body=body, params=params)
        else:
            raise exceptions.FailedExecution(
                'Unable to create repository {0}.  '
                'A repository with that name already exists.'.format(repository)
            )
    except elasticsearch.TransportError as err:
        raise exceptions.FailedExecution(
            """
            Unable to create repository {0}.  Response Code: {1}.  Error: {2}.
            Check curator and elasticsearch logs for more information.
            """.format(repository, err.status_code, err.error)
        )
    LOGGER.debug("Repository {0} creation initiated...".format(repository))
    return True

def repository_exists(client, repository=None):
    """
    Verify the existence of a repository

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: bool
    """
    if not repository:
        raise exceptions.MissingArgument('No value for "repository" provided')
    try:
        test_result = get_repository(client, repository)
        if repository in test_result:
            LOGGER.debug("Repository {0} exists.".format(repository))
            response = True
        else:
            LOGGER.debug("Repository {0} not found...".format(repository))
            response = False
    except Exception as err:
        LOGGER.debug('Unable to find repository "{0}": Error: {1}'.format(repository, err))
        response = False
    return response

def test_repo_fs(client, repository=None):
    """
    Test whether all nodes have write access to the repository

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    """
    try:
        nodes = client.snapshot.verify_repository(
            repository=repository)['nodes']
        LOGGER.debug('All nodes can write to the repository')
        LOGGER.debug('Nodes with verified repository access: {0}'.format(nodes))
    except Exception as err:
        try:
            if err.status_code == 404:
                msg = (
                    '--- Repository "{0}" not found. Error: '
                    '{1}, {2}'.format(repository, err.status_code, err.error)
                )
            else:
                msg = (
                    '--- Got a {0} response from Elasticsearch.  '
                    'Error message: {1}'.format(err.status_code, err.error)
                )
        except AttributeError:
            msg = ('--- Error message: {0}'.format(err))
        raise exceptions.ActionError(
            'Failed to verify all nodes have repository access: {0}'.format(msg)
        )

def snapshot_running(client):
    """
    Return `True` if a snapshot is in progress, and `False` if not

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    try:
        status = client.snapshot.status()['snapshots']
    except Exception as err:
        report_failure(err)
    # We will only accept a positively identified False.  Anything else is
    # suspect.
    return False if not status else True

def parse_date_pattern(name):
    """
    Scan and parse `name` for :py:func:`time.strftime` strings, replacing them
    with the associated value when found, but otherwise returning lowercase
    values, as uppercase snapshot names are not allowed. It will detect if the
    first character is a `<`, which would indicate `name` is going to be using
    Elasticsearch date math syntax, and skip accordingly.

    The :py:func:`time.strftime` identifiers that Curator currently recognizes
    as acceptable include:

    * ``Y``: A 4 digit year
    * ``y``: A 2 digit year
    * ``m``: The 2 digit month
    * ``W``: The 2 digit week of the year
    * ``d``: The 2 digit day of the month
    * ``H``: The 2 digit hour of the day, in 24 hour notation
    * ``M``: The 2 digit minute of the hour
    * ``S``: The 2 digit number of second of the minute
    * ``j``: The 3 digit day of the year

    :arg name: A name, which can contain :py:func:`time.strftime`
        strings
    """
    prev, rendered = ('', '')
    LOGGER.debug('Provided index name: {0}'.format(name))
    for idx, char in enumerate(name):
        LOGGER.debug('Current character in provided name: {0}, position: {1}'.format(char, idx))
        if char == '<':
            LOGGER.info('"{0}" is probably using Elasticsearch date math.'.format(name))
            rendered = name
            break
        if char == '%':
            pass
        elif char in settings.date_regex() and prev == '%':
            rendered += str(datetime.utcnow().strftime('%{0}'.format(char)))
        else:
            rendered += char
        LOGGER.debug('Partially rendered name: {0}'.format(rendered))
        prev = char
    LOGGER.debug('Fully rendered name: {0}'.format(rendered))
    return rendered

def prune_nones(mydict):
    """
    Remove keys from `mydict` whose values are `None`

    :arg mydict: The dictionary to act on
    :rtype: dict
    """
    # Test for `None` instead of existence or zero values will be caught
    return dict([(k, v) for k, v in mydict.items() if v is not None and v != 'None'])

def validate_filters(action, filters):
    """
    Validate that the filters are appropriate for the action type, e.g. no
    index filters applied to a snapshot list.

    :arg action: An action name
    :arg filters: A list of filters to test.
    """
    # Define which set of filtertypes to use for testing
    if action in settings.snapshot_actions():
        filtertypes = settings.snapshot_filtertypes()
    else:
        filtertypes = settings.index_filtertypes()
    for fil in filters:
        if fil['filtertype'] not in filtertypes:
            raise exceptions.ConfigurationError(
                '"{0}" filtertype is not compatible with action "{1}"'.format(
                    fil['filtertype'],
                    action
                )
            )
    # If we get to this point, we're still valid.  Return the original list
    return filters

def validate_actions(data):
    """
    Validate an Action configuration dictionary, as imported from actions.yml,
    for example.

    The method returns a validated and sanitized configuration dictionary.

    :arg data: The configuration dictionary
    :rtype: dict
    """
    # data is the ENTIRE schema...
    clean_config = {}
    # Let's break it down into smaller chunks...
    # First, let's make sure it has "actions" as a key, with a subdictionary
    root = SchemaCheck(data, actions.root(), 'Actions File', 'root').result()
    # We've passed the first step.  Now let's iterate over the actions...
    for action_id in root['actions']:
        # Now, let's ensure that the basic action structure is correct, with
        # the proper possibilities for 'action'
        action_dict = root['actions'][action_id]
        loc = 'Action ID "{0}"'.format(action_id)
        valid_structure = SchemaCheck(
            action_dict,
            actions.structure(action_dict, loc),
            'structure',
            loc
        ).result()
        # With the basic structure validated, now we extract the action name
        current_action = valid_structure['action']
        # And let's update the location with the action.
        loc = 'Action ID "{0}", action "{1}"'.format(
            action_id, current_action)
        clean_options = SchemaCheck(
            prune_nones(valid_structure['options']),
            options.get_schema(current_action),
            'options',
            loc
        ).result()
        clean_config[action_id] = {
            'action' : current_action,
            'description' : valid_structure['description'],
            'options' : clean_options,
        }
        if current_action == 'alias':
            add_remove = {}
            for k in ['add', 'remove']:
                if k in valid_structure:
                    current_filters = SchemaCheck(
                        valid_structure[k]['filters'],
                        Schema(filters.Filters(current_action, location=loc)),
                        '"{0}" filters'.format(k),
                        '{0}, "filters"'.format(loc)
                    ).result()
                    add_remove.update(
                        {
                            k: {
                                'filters' : SchemaCheck(
                                    current_filters,
                                    Schema(filters.Filters(current_action, location=loc)),
                                    'filters',
                                    '{0}, "{1}", "filters"'.format(loc, k)
                                    ).result()
                                }
                        }
                    )
            # Add/Remove here
            clean_config[action_id].update(add_remove)
        elif current_action in ['cluster_routing', 'create_index', 'rollover']:
            # neither cluster_routing nor create_index should have filters
            pass
        else: # Filters key only appears in non-alias actions
            valid_filters = SchemaCheck(
                valid_structure['filters'],
                Schema(filters.Filters(current_action, location=loc)),
                'filters',
                '{0}, "filters"'.format(loc)
            ).result()
            clean_filters = validate_filters(current_action, valid_filters)
            clean_config[action_id].update({'filters' : clean_filters})
        # This is a special case for remote reindex
        if current_action == 'reindex':
            # Check only if populated with something.
            if 'remote_filters' in valid_structure['options']:
                valid_filters = SchemaCheck(
                    valid_structure['options']['remote_filters'],
                    Schema(filters.Filters(current_action, location=loc)),
                    'filters',
                    '{0}, "filters"'.format(loc)
                ).result()
                clean_remote_filters = validate_filters(current_action, valid_filters)
                clean_config[action_id]['options'].update({'remote_filters': clean_remote_filters})

    # if we've gotten this far without any Exceptions raised, it's valid!
    return {'actions': clean_config}

def health_check(client, **kwargs):
    """
    This function calls client.cluster.health and, based on the args provided,
    will return `True` or `False` depending on whether that particular keyword
    appears in the output, and has the expected value.
    If multiple keys are provided, all must match for a `True` response.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    """
    LOGGER.debug('KWARGS= "{0}"'.format(kwargs))
    klist = list(kwargs.keys())
    if not klist:
        raise exceptions.MissingArgument('Must provide at least one keyword argument')
    hc_data = client.cluster.health()
    response = True

    for k in klist:
        # First, verify that all kwargs are in the list
        if not k in list(hc_data.keys()):
            raise exceptions.ConfigurationError('Key "{0}" not in cluster health output')
        if not hc_data[k] == kwargs[k]:
            LOGGER.debug(
                'NO MATCH: Value for key "{0}", health check data: '
                '{1}'.format(kwargs[k], hc_data[k])
            )
            response = False
        else:
            LOGGER.debug(
                'MATCH: Value for key "{0}", health check data: '
                '{1}'.format(kwargs[k], hc_data[k])
            )
    if response:
        LOGGER.info('Health Check for all provided keys passed.')
    return response

def snapshot_check(client, snapshot=None, repository=None):
    """
    This function calls `client.snapshot.get` and tests to see whether the
    snapshot is complete, and if so, with what status.  It will log errors
    according to the result. If the snapshot is still `IN_PROGRESS`, it will
    return `False`.  `SUCCESS` will be an `INFO` level message, `PARTIAL` nets
    a `WARNING` message, `FAILED` is an `ERROR`, message, and all others will be
    a `WARNING` level message.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg snapshot: The name of the snapshot.
    :arg repository: The Elasticsearch snapshot repository to use
    """
    try:
        state = client.snapshot.get(
            repository=repository, snapshot=snapshot)['snapshots'][0]['state']
    except Exception as err:
        raise exceptions.CuratorException(
            'Unable to obtain information for snapshot "{0}" in repository '
            '"{1}". Error: {2}'.format(snapshot, repository, err)
        )
    LOGGER.debug('Snapshot state = {0}'.format(state))
    if state == 'IN_PROGRESS':
        LOGGER.info('Snapshot {0} still in progress.'.format(snapshot))
        return False
    elif state == 'SUCCESS':
        LOGGER.info(
            'Snapshot {0} successfully completed.'.format(snapshot))
    elif state == 'PARTIAL':
        LOGGER.warn(
            'Snapshot {0} completed with state PARTIAL.'.format(snapshot))
    elif state == 'FAILED':
        LOGGER.error(
            'Snapshot {0} completed with state FAILED.'.format(snapshot))
    else:
        LOGGER.warn(
            'Snapshot {0} completed with state: {0}'.format(snapshot))
    return True

def relocate_check(client, index):
    """
    This function calls client.cluster.state with a given index to check if
    all of the shards for that index are in the STARTED state. It will
    return `True` if all shards both primary and replica are in the STARTED
    state, and it will return `False` if any primary or replica shard is in
    a different state.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg index: The index to check the index shards state.
    """
    shard_state_data = (
        client.cluster.state(index=index)['routing_table']['indices'][index]['shards']
    )
    finished_state = (
        all(
            all(
                shard['state'] == "STARTED" for shard in shards
            )
            for shards in shard_state_data.values()
        )
    )
    if finished_state:
        LOGGER.info('Relocate Check for index: "{0}" has passed.'.format(index))
    return finished_state


def restore_check(client, index_list):
    """
    This function calls client.indices.recovery with the list of indices to
    check for complete recovery.  It will return `True` if recovery of those
    indices is complete, and `False` otherwise.  It is designed to fail fast:
    if a single shard is encountered that is still recovering (not in `DONE`
    stage), it will immediately return `False`, rather than complete iterating
    over the rest of the response.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg index_list: The list of indices to verify having been restored.
    """
    response = {}
    for chunk in chunk_index_list(index_list):
        try:
            chunk_response = client.indices.recovery(index=chunk, human=True)
        except Exception as err:
            raise exceptions.CuratorException(
                'Unable to obtain recovery information for specified indices. '
                'Error: {0}'.format(err)
            )
        # This should address #962, where perhaps the cluster state hasn't yet
        # had a chance to add a _recovery state yet, so it comes back empty.
        if chunk_response == {}:
            LOGGER.info('_recovery returned an empty response. Trying again.')
            return False
        response.update(chunk_response)
    # Fixes added in #989
    LOGGER.info('Provided indices: {0}'.format(index_list))
    LOGGER.info('Found indices: {0}'.format(list(response.keys())))
    for index in response:
        for shard in range(0, len(response[index]['shards'])):
            # Apparently `is not` is not always `!=`.  Unsure why, will
            # research later.  Using != fixes #966
            if response[index]['shards'][shard]['stage'] != 'DONE':
                LOGGER.info(
                    'Index "{0}" is still in stage "{1}"'.format(
                        index, response[index]['shards'][shard]['stage']
                    )
                )
                return False
    # If we've gotten here, all of the indices have recovered
    return True


def task_check(client, task_id=None):
    """
    This function calls client.tasks.get with the provided `task_id`.  If the
    task data contains ``'completed': True``, then it will return `True`
    If the task is not completed, it will log some information about the task
    and return `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg task_id: A task_id which ostensibly matches a task searchable in the
        tasks API.
    """
    try:
        task_data = client.tasks.get(task_id=task_id)
    except Exception as err:
        raise exceptions.CuratorException(
            'Unable to obtain task information for task_id "{0}". Exception '
            '{1}'.format(task_id, err)
        )
    task = task_data['task']
    completed = task_data['completed']
    if task['action'] == 'indices:data/write/reindex':
        LOGGER.debug('It\'s a REINDEX TASK')
        LOGGER.debug('TASK_DATA: {0}'.format(task_data))
        LOGGER.debug('TASK_DATA keys: {0}'.format(list(task_data.keys())))
        if 'response' in task_data:
            response = task_data['response']
            if response['failures']:
                raise exceptions.FailedReindex(
                    'Failures found in reindex response: {0}'.format(response['failures'])
                )
    running_time = 0.000000001 * task['running_time_in_nanos']
    LOGGER.debug('Running time: {0} seconds'.format(running_time))
    descr = task['description']

    if completed:
        completion_time = ((running_time * 1000) + task['start_time_in_millis'])
        time_string = time.strftime(
            '%Y-%m-%dT%H:%M:%SZ', time.localtime(completion_time/1000)
        )
        LOGGER.info('Task "{0}" completed at {1}.'.format(descr, time_string))
        return True
    else:
        # Log the task status here.
        LOGGER.debug('Full Task Data: {0}'.format(task_data))
        LOGGER.info(
            'Task "{0}" with task_id "{1}" has been running for '
            '{2} seconds'.format(descr, task_id, running_time))
        return False


def wait_for_it(
        client, action, task_id=None, snapshot=None, repository=None,
        index=None, index_list=None, wait_interval=9, max_wait=-1
    ):
    """
    This function becomes one place to do all wait_for_completion type behaviors

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg action: The action name that will identify how to wait
    :arg task_id: If the action provided a task_id, this is where it must be
        declared.
    :arg snapshot: The name of the snapshot.
    :arg repository: The Elasticsearch snapshot repository to use
    :arg wait_interval: How frequently the specified "wait" behavior will be
        polled to check for completion.
    :arg max_wait: Number of seconds will the "wait" behavior persist
        before giving up and raising an Exception.  The default is -1, meaning
        it will try forever.
    """
    action_map = {
        'allocation':{
            'function': health_check,
            'args': {'relocating_shards':0},
        },
        'replicas':{
            'function': health_check,
            'args': {'status':'green'},
        },
        'cluster_routing':{
            'function': health_check,
            'args': {'relocating_shards':0},
        },
        'snapshot':{
            'function':snapshot_check,
            'args':{'snapshot':snapshot, 'repository':repository},
        },
        'restore':{
            'function':restore_check,
            'args':{'index_list':index_list},
        },
        'reindex':{
            'function':task_check,
            'args':{'task_id':task_id},
        },
        'shrink':{
            'function': health_check,
            'args': {'status':'green'},
        },
        'relocate':{
            'function': relocate_check,
            'args': {'index':index}
        },
    }
    wait_actions = list(action_map.keys())

    if action not in wait_actions:
        raise exceptions.ConfigurationError(
            '"action" must be one of {0}'.format(wait_actions)
        )
    if action == 'reindex' and task_id is None:
        raise exceptions.MissingArgument(
            'A task_id must accompany "action" {0}'.format(action)
        )
    if action == 'snapshot' and ((snapshot is None) or (repository is None)):
        raise exceptions.MissingArgument(
            'A snapshot and repository must accompany "action" {0}. snapshot: '
            '{1}, repository: {2}'.format(action, snapshot, repository)
        )
    if action == 'restore' and index_list is None:
        raise exceptions.MissingArgument(
            'An index_list must accompany "action" {0}'.format(action)
        )
    elif action == 'reindex':
        try:
            _ = client.tasks.get(task_id=task_id)
        except Exception as err:
            # This exception should only exist in API usage. It should never
            # occur in regular Curator usage.
            raise exceptions.CuratorException(
                'Unable to find task_id {0}. Exception: {1}'.format(task_id, err)
            )

    # Now with this mapped, we can perform the wait as indicated.
    start_time = datetime.now()
    result = False
    while True:
        elapsed = int((datetime.now() - start_time).total_seconds())
        LOGGER.debug('Elapsed time: {0} seconds'.format(elapsed))
        response = action_map[action]['function'](
            client, **action_map[action]['args'])
        LOGGER.debug('Response: {0}'.format(response))
        # Success
        if response:
            LOGGER.debug(
                'Action "{0}" finished executing (may or may not have been '
                'successful)'.format(action))
            result = True
            break
        # Not success, and reached maximum wait (if defined)
        elif (max_wait != -1) and (elapsed >= max_wait):
            LOGGER.error(
                'Unable to complete action "{0}" within max_wait ({1}) '
                'seconds.'.format(action, max_wait)
            )
            break
        # Not success, so we wait.
        else:
            LOGGER.debug(
                'Action "{0}" not yet complete, {1} total seconds elapsed. '
                'Waiting {2} seconds before checking '
                'again.'.format(action, elapsed, wait_interval))
            time.sleep(wait_interval)

    LOGGER.debug('Result: {0}'.format(result))
    if not result:
        raise exceptions.ActionTimeout(
            'Action "{0}" failed to complete in the max_wait period of '
            '{1} seconds'.format(action, max_wait)
        )

def node_roles(client, node_id):
    """
    Return the list of roles assigned to the node identified by ``node_id``

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: list
    """
    return client.nodes.info()['nodes'][node_id]['roles']

def index_size(client, idx, value='total'):
    """
    Return the sum of either `primaries` or `total` shards for index ``idx``

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg idx: An Elasticsearch index
    :arg value: One of either `primaries` or `total`
    :rtype: integer
    """
    return client.indices.stats(index=idx)['indices'][idx][value]['store']['size_in_bytes']

def single_data_path(client, node_id):
    """
    In order for a shrink to work, it should be on a single filesystem, as
    shards cannot span filesystems.  Return `True` if the node has a single
    filesystem, and `False` otherwise.

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    return len(client.nodes.stats()['nodes'][node_id]['fs']['data']) == 1


def name_to_node_id(client, name):
    """
    Return the node_id of the node identified by ``name``

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: str
    """
    stats = client.nodes.stats()
    for node in stats['nodes']:
        if stats['nodes'][node]['name'] == name:
            LOGGER.debug('Found node_id "{0}" for name "{1}".'.format(node, name))
            return node
    LOGGER.error('No node_id found matching name: "{0}"'.format(name))
    return None

def node_id_to_name(client, node_id):
    """
    Return the name of the node identified by ``node_id``

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: str
    """
    stats = client.nodes.stats()
    name = None
    if node_id in stats['nodes']:
        name = stats['nodes'][node_id]['name']
    else:
        LOGGER.error('No node_id found matching: "{0}"'.format(node_id))
    LOGGER.debug('Name associated with node_id "{0}": {1}'.format(node_id, name))
    return name

def get_datemath(client, datemath, random_element=None):
    """
    Return the parsed index name from ``datemath``
    """
    if random_element is None:
        random_prefix = (
            'curator_get_datemath_function_' +
            ''.join(random.choice(string.ascii_lowercase) for _ in range(32))
        )
    else:
        random_prefix = 'curator_get_datemath_function_' + random_element
    datemath_dummy = '<{0}-{1}>'.format(random_prefix, datemath)
    # We both want and expect a 404 here (NotFoundError), since we have
    # created a 32 character random string to definitely be an unknown
    # index name.
    LOGGER.debug('Random datemath string for extraction: {0}'.format(datemath_dummy))
    try:
        client.indices.get(index=datemath_dummy)
    except elasticsearch.exceptions.NotFoundError as err:
        # This is the magic.  Elasticsearch still gave us the formatted
        # index name in the error results.
        faux_index = err.info['error']['index']
    LOGGER.debug('Response index name for extraction: {0}'.format(faux_index))
    # Now we strip the random index prefix back out again
    pattern = r'^{0}-(.*)$'.format(random_prefix)
    regex = re.compile(pattern)
    try:
        # And return only the now-parsed date string
        return regex.match(faux_index).group(1)
    except AttributeError:
        raise exceptions.ConfigurationError(
            'The rendered index "{0}" does not contain a valid date pattern '
            'or has invalid index name characters.'.format(faux_index)
        )

def isdatemath(data):
    """Check if data is a datemath expression"""
    initial_check = r'^(.).*(.)$'
    regex = re.compile(initial_check)
    opener = regex.match(data).group(1)
    closer = regex.match(data).group(2)
    LOGGER.debug('opener =  {0}, closer = {1}'.format(opener, closer))
    if (opener == '<' and closer != '>') or (opener != '<' and closer == '>'):
        raise exceptions.ConfigurationError('Incomplete datemath encapsulation in "< >"')
    elif (opener != '<' and closer != '>'):
        return False
    return True

def parse_datemath(client, value):
    """
    Check if ``value`` is datemath.
    Parse it if it is.
    Return the bare value otherwise.
    """
    if not isdatemath(value):
        return value
    else:
        LOGGER.debug('Properly encapsulated, proceeding to next evaluation...')
    # Our pattern has 4 capture groups.
    # 1. Everything after the initial '<' up to the first '{', which we call ``prefix``
    # 2. Everything between the outermost '{' and '}', which we call ``datemath``
    # 3. An optional inner '{' and '}' containing a date formatter and potentially a timezone.
    #    Not captured.
    # 4. Everything after the last '}' up to the closing '>'
    pattern = r'^<([^\{\}]*)?(\{.*(\{.*\})?\})([^\{\}]*)?>$'
    regex = re.compile(pattern)
    try:
        prefix = regex.match(value).group(1) or ''
        datemath = regex.match(value).group(2)
        # formatter = regex.match(value).group(3) or '' (not captured, but counted)
        suffix = regex.match(value).group(4) or ''
    except AttributeError:
        raise exceptions.ConfigurationError(
            'Value "{0}" does not contain a valid datemath pattern.'.format(value))
    return '{0}{1}{2}'.format(prefix, get_datemath(client, datemath), suffix)

def get_write_index(client, alias):
    """Find which index associated with an alias is the write index"""
    try:
        response = client.indices.get_alias(index=alias)
    except:
        raise exceptions.CuratorException('Alias {0} not found'.format(alias))
    # If there are more than one in the list, one needs to be the write index
    # otherwise the alias is a one to many, and can't do rollover.
    if len(list(response.keys())) > 1:
        for index in list(response.keys()):
            try:
                if response[index]['aliases'][alias]['is_write_index']:
                    return index
            except KeyError:
                raise exceptions.FailedExecution(
                    'Invalid alias: is_write_index not found in 1 to many alias')
    else:
        # There's only one, so this is it
        return list(response.keys())[0]
