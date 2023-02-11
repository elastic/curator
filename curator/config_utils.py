"""Configuration utilty functions"""
import logging
from copy import deepcopy
import click
from es_client.helpers.utils import prune_nones, ensure_list
from curator.defaults.logging_defaults import config_logging
from curator.validators.schemacheck import SchemaCheck
from curator.logtools import LogInfo, Blacklist

def check_logging_config(config):
    """
    Ensure that the top-level key ``logging`` is in ``config`` before passing it to
    :py:class:`~.curator.validators.schemacheck.SchemaCheck` for value validation.

    :param config: Logging configuration data

    :type config: dict

    :returns: :py:class:`~.curator.validators.schemacheck.SchemaCheck` validated logging
        configuration.
    """

    if not isinstance(config, dict):
        click.echo(
            f'Must supply logging information as a dictionary. '
            f'You supplied: "{config}" which is "{type(config)}"'
            f'Using default logging values.'
        )
        log_settings = {}
    elif not 'logging' in config:
        click.echo('No "logging" setting in supplied configuration.  Using defaults.')
        log_settings = {}
    else:
        if config['logging']:
            log_settings = prune_nones(config['logging'])
        else:
            log_settings = {}
    return SchemaCheck(
        log_settings, config_logging(), 'Logging Configuration', 'logging').result()

def set_logging(log_opts):
    """Configure global logging options

    :param log_opts: Logging configuration data

    :type log_opts: dict

    :rtype: None
    """
    # Set up logging
    loginfo = LogInfo(log_opts)
    logging.root.addHandler(loginfo.handler)
    logging.root.setLevel(loginfo.numeric_log_level)
    _ = logging.getLogger('curator.cli')
    # Set up NullHandler() to handle nested elasticsearch8.trace Logger
    # instance in elasticsearch python client
    logging.getLogger('elasticsearch8.trace').addHandler(logging.NullHandler())
    if log_opts['blacklist']:
        for bl_entry in ensure_list(log_opts['blacklist']):
            for handler in logging.root.handlers:
                handler.addFilter(Blacklist(bl_entry))

def password_filter(data):
    """
    Recursively look through all nested structures of ``data`` for the key ``'password'`` and redact
    the value.

    :param data: Configuration data

    :type data: dict

    :returns: A :py:class:`~.copy.deepcopy` of ``data`` with the value obscured by ``REDACTED``
        if the key is ``'password'``.
    """
    def iterdict(mydict):
        for key, value in mydict.items():
            if isinstance(value, dict):
                iterdict(value)
            elif key == "password":
                mydict.update({"password": "REDACTED"})
        return mydict
    return iterdict(deepcopy(data))
