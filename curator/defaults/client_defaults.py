from voluptuous import *

# Configuration file: client
def config_client():
    return {
        Optional('hosts', default='127.0.0.1'): Any(None, str, unicode, list),
        Optional('port', default=9200): Any(
            None, All(Coerce(int), Range(min=1, max=65535))
        ),
        Optional('url_prefix', default=''): Any(None, str, unicode),
        Optional('use_ssl', default=False): Boolean(),
        Optional('certificate', default=None): Any(None, str, unicode),
        Optional('client_cert', default=None): Any(None, str, unicode),
        Optional('client_key', default=None): Any(None, str, unicode),
        Optional('aws_key', default=None): Any(None, str, unicode),
        Optional('aws_secret_key', default=None): Any(None, str, unicode),
        Optional('aws_token', default=None): Any(None, str, unicode),
        Optional('aws_sign_request', default=False): Boolean(),
        Optional('aws_region'): Any(None, str, unicode),
        Optional('ssl_no_validate', default=False): Boolean(),
        Optional('http_auth', default=None): Any(None, str, unicode),
        Optional('timeout', default=30): All(
            Coerce(int), Range(min=1, max=86400)),
        Optional('master_only', default=False): Boolean(),
    }

# Configuration file: logging
def config_logging():
    return {
        Optional(
            'loglevel', default='INFO'): Any(None,
            'NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL',
            All(Coerce(int), Any(0, 10, 20, 30, 40, 50))
        ),
        Optional('logfile', default=None): Any(None, str, unicode),
        Optional(
            'logformat', default='default'): Any(None, All(
                Any(str, unicode),
                Any('default', 'json', 'logstash')
            )
        ),
        Optional(
            'blacklist', default=['elasticsearch', 'urllib3']): Any(None, list),
    }