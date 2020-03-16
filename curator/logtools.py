"""Logging tools"""
import sys
import json
import logging
import time
from curator.exceptions import LoggingException

def de_dot(dot_string, msg):
    """Turn message and dotted string into a nested dictionary"""
    arr = dot_string.split('.')
    arr.append(msg)
    retval = None
    for idx in range(len(arr), 1, -1):
        if not retval:
            try:
                retval = {arr[idx-2]: arr[idx-1]}
            except Exception as err:
                raise LoggingException(err)
        else:
            try:
                new_d = {arr[idx-2]: retval}
                retval = new_d
            except Exception as err:
                raise LoggingException(err)
    return retval

def deepmerge(source, destination):
    """Merge deeply nested dictionary structures"""
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            deepmerge(value, node)
        else:
            destination[key] = value
    return destination
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

class ECSFormatter(LogstashFormatter):
    """Elastic Common Schema formatting (ECS)"""
    # Overload LogstashFormatter attribute
    WANTED_ATTRS = {
        'levelname': 'log.level',
        'funcName': 'log.origin.function',
        'lineno': 'log.origin.file.line',
        'message': 'log.original',
        'name': 'log.logger'
    }

class Whitelist(logging.Filter):
    """How to whitelist logs"""
    def __init__(self, *whitelist):
        self.whitelist = [logging.Filter(name) for name in whitelist]

    def filter(self, record):
        return any(f.filter(record) for f in self.whitelist)

class Blacklist(Whitelist):
    """Blacklist monkey-patch of Whitelist"""
    def filter(self, record):
        return not Whitelist.filter(self, record)

class LogInfo(object):
    """Logging Class"""
    def __init__(self, cfg):
        cfg['loglevel'] = 'INFO' if not 'loglevel' in cfg else cfg['loglevel']
        cfg['logfile'] = None if not 'logfile' in cfg else cfg['logfile']
        cfg['logformat'] = 'default' if not 'logformat' in cfg else cfg['logformat']
        self.numeric_log_level = getattr(logging, cfg['loglevel'].upper(), None)
        self.format_string = '%(asctime)s %(levelname)-9s %(message)s'
        if not isinstance(self.numeric_log_level, int):
            raise ValueError('Invalid log level: {0}'.format(cfg['loglevel']))

        self.handler = logging.StreamHandler(
            open(cfg['logfile'], 'a') if cfg['logfile'] else sys.stdout
        )

        if self.numeric_log_level == 10: # DEBUG
            self.format_string = (
                '%(asctime)s %(levelname)-9s %(name)22s '
                '%(funcName)22s:%(lineno)-4d %(message)s'
            )

        if cfg['logformat'] == 'json' or cfg['logformat'] == 'logstash':
            self.handler.setFormatter(LogstashFormatter())
        elif cfg['logformat'] == 'ecs':
            self.handler.setFormatter(ECSFormatter())
        else:
            self.handler.setFormatter(logging.Formatter(self.format_string))
