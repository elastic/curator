"""Define valid schemas for client configuration validation"""
from six import string_types
from voluptuous import All, Any, Coerce, Optional, Schema

# Configuration file: logging
def config_logging():
    """
    Logging schema with defaults:

    .. code-block:: yaml

        logging:
          loglevel: INFO
          logfile: None
          logformat: default
          blacklist: ['elastic_transport', 'urllib3']

    :returns: A valid :py:class:`~.voluptuous.schema_builder.Schema` of all acceptable values with
        the default values set.
    :rtype: :py:class:`~.voluptuous.schema_builder.Schema`
    """
    return Schema(
        {
            Optional('loglevel', default='INFO'):
                Any(None, 'NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL',
                    All(Coerce(int), Any(0, 10, 20, 30, 40, 50))
                    ),
            Optional('logfile', default=None): Any(None, *string_types),
            Optional('logformat', default='default'):
                Any(None, All(Any(*string_types), Any('default', 'json', 'logstash', 'ecs'))),
            Optional('blacklist', default=['elastic_transport', 'urllib3']): Any(None, list),
        }
    )
