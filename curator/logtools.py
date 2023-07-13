"""Logging tools"""
import sys
import json
import logging
import time
from pathlib import Path
import ecs_logging
from curator.exceptions import LoggingException

def de_dot(dot_string, msg):
    """
    Turn message and dotted string into a nested dictionary. Used by :py:class:`LogstashFormatter`

    :param dot_string: The dotted string
    :param msg: The message

    :type dot_string: str
    :type msg: str
    """
    arr = dot_string.split('.')
    arr.append(msg)
    retval = None
    for idx in range(len(arr), 1, -1):
        if not retval:
            try:
                retval = {arr[idx-2]: arr[idx-1]}
            except Exception as err:
                raise LoggingException(err) from err
        else:
            try:
                new_d = {arr[idx-2]: retval}
                retval = new_d
            except Exception as err:
                raise LoggingException(err) from err
    return retval

def deepmerge(source, destination):
    """
    Recursively merge deeply nested dictionary structures, ``source`` into ``destination``

    :param source: Source dictionary
    :param destination: Destination dictionary

    :type source: dict
    :type destination: dict

    :returns: destination
    :rtype: dict
    """
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            deepmerge(value, node)
        else:
            destination[key] = value
    return destination

def is_docker():
    """Check if we're running in a docker container"""
    cgroup = Path('/proc/self/cgroup')
    return Path('/.dockerenv').is_file() or cgroup.is_file() and 'docker' in cgroup.read_text()

class LogstashFormatter(logging.Formatter):
    """Logstash formatting (JSON)"""
    # The LogRecord attributes we want to carry over to the Logstash message,
    # mapped to the corresponding output key.
    WANTED_ATTRS = {
        'levelname': 'loglevel',
        'funcName': 'function',
        'lineno': 'linenum',
        'message': 'message',
        'name': 'name'
    }

    def format(self, record):
        """
        :param record: The incoming log message

        :rtype: :py:meth:`json.dumps`
        """
        self.converter = time.gmtime
        timestamp = '%s.%03dZ' % (
            self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S'), record.msecs)
        result = {'@timestamp': timestamp}
        available = record.__dict__
        # This is cleverness because 'message' is NOT a member key of ``record.__dict__``
        # the ``getMessage()`` method is effectively ``msg % args`` (actual keys)
        # By manually adding 'message' to ``available``, it simplifies the code
        available['message'] = record.getMessage()
        for attribute in set(self.WANTED_ATTRS).intersection(available):
            result = deepmerge(
                de_dot(self.WANTED_ATTRS[attribute], getattr(record, attribute)), result
            )
        # The following is mostly for the ecs format. You can't have 2x 'message' keys in
        # WANTED_ATTRS, so we set the value to 'log.original' in ecs, and this code block
        # guarantees it still appears as 'message' too.
        if 'message' not in result.items():
            result['message'] = available['message']
        return json.dumps(result, sort_keys=True)

class Whitelist(logging.Filter):
    """How to whitelist logs"""
    # pylint: disable=super-init-not-called
    def __init__(self, *whitelist):
        self.whitelist = [logging.Filter(name) for name in whitelist]

    def filter(self, record):
        return any(f.filter(record) for f in self.whitelist)

class Blacklist(Whitelist):
    """Blacklist monkey-patch of Whitelist"""
    def filter(self, record):
        return not Whitelist.filter(self, record)

class LogInfo:
    """Logging Class"""
    def __init__(self, cfg):
        """Class Setup

        :param cfg: The logging configuration
        :type: cfg: dict
        """
        cfg['loglevel'] = 'INFO' if not 'loglevel' in cfg else cfg['loglevel']
        cfg['logfile'] = None if not 'logfile' in cfg else cfg['logfile']
        cfg['logformat'] = 'default' if not 'logformat' in cfg else cfg['logformat']
        #: Attribute. The numeric equivalent of ``cfg['loglevel']``
        self.numeric_log_level = getattr(logging, cfg['loglevel'].upper(), None)
        #: Attribute. The logging format string to use.
        self.format_string = '%(asctime)s %(levelname)-9s %(message)s'

        if not isinstance(self.numeric_log_level, int):
            raise ValueError(f"Invalid log level: {cfg['loglevel']}")

        #: Attribute. Which logging handler to use
        if is_docker():
            self.handler = logging.FileHandler('/proc/1/fd/1')
        else:
            self.handler = logging.StreamHandler(stream=sys.stdout)
        if cfg['logfile']:
            self.handler = logging.FileHandler(cfg['logfile'])

        if self.numeric_log_level == 10: # DEBUG
            self.format_string = (
                '%(asctime)s %(levelname)-9s %(name)22s %(funcName)22s:%(lineno)-4d %(message)s')

        if cfg['logformat'] == 'json' or cfg['logformat'] == 'logstash':
            self.handler.setFormatter(LogstashFormatter())
        elif cfg['logformat'] == 'ecs':
            self.handler.setFormatter(ecs_logging.StdlibFormatter())
        else:
            self.handler.setFormatter(logging.Formatter(self.format_string))
