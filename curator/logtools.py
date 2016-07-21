import sys
import json
import logging
import time

class LogstashFormatter(logging.Formatter):
    # The LogRecord attributes we want to carry over to the Logstash message,
    # mapped to the corresponding output key.
    WANTED_ATTRS = {'levelname': 'loglevel',
                    'funcName': 'function',
                    'lineno': 'linenum',
                    'message': 'message',
                    'name': 'name'}

    # def converter(self, timevalue):
    #     return time.gmtime(timevalue)

    def format(self, record):
        self.converter = time.gmtime
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

class Blacklist(Whitelist):
    def filter(self, record):
        return not Whitelist.filter(self, record)

class LogInfo(object):
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
        else:
            self.handler.setFormatter(logging.Formatter(self.format_string))
