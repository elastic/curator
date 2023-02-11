"""Logging Schema definition"""
from voluptuous import Optional, Schema
from curator.defaults import logging_defaults

def logging():
    """
    Pulls value from :py:func:`~.curator.defaults.logging_defaults.config_logging`

    :returns: ``{Optional('logging'): logging_defaults.config_logging()}``
    :rtype: :py:class:`~.voluptuous.schema_builder.Schema`
    """
    return Schema({Optional('logging'): logging_defaults.config_logging()})
