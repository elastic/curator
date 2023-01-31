"""Define valid schemas for client configuration validation"""
from six import string_types
from voluptuous import All, Any, Coerce, Optional, Schema

# Configuration file: logging
def config_logging():
    """Logging schema"""
    return Schema(
        {
            Optional('loglevel', default='INFO'):
                Any(None, 'NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL',
                    All(Coerce(int), Any(0, 10, 20, 30, 40, 50))
                    ),
            Optional('logfile', default=None): Any(None, *string_types),
            Optional('logformat', default='default'):
                Any(None, All(Any(*string_types), Any('default', 'json', 'logstash', 'ecs'))),
            Optional('blacklist', default=['elasticsearch', 'urllib3']): Any(None, list),
        }
    )
