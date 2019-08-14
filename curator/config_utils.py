import logging
from voluptuous import Schema
from curator.validators import SchemaCheck, config_file
from curator.utils import ensure_list, get_yaml, prune_nones, test_client_options
from curator.logtools import LogInfo, Whitelist, Blacklist
from copy import deepcopy

def test_config(config):
    # Get config from yaml file
    yaml_config  = get_yaml(config)
    # if the file is empty, which is still valid yaml, set as an empty dict
    yaml_config = {} if not yaml_config else prune_nones(yaml_config)
    # Voluptuous can't verify the schema of a dict if it doesn't have keys,
    # so make sure the keys are at least there and are dict()
    for k in ['client', 'logging']:
        if k not in yaml_config:
            yaml_config[k] = {}
        else:
            yaml_config[k] = prune_nones(yaml_config[k])
    return SchemaCheck(yaml_config, config_file.client(),
        'Client Configuration', 'full configuration dictionary').result()

def set_logging(log_opts):
    # Set up logging
    loginfo = LogInfo(log_opts)
    logging.root.addHandler(loginfo.handler)
    logging.root.setLevel(loginfo.numeric_log_level)
    _ = logging.getLogger('curator.cli')
    # Set up NullHandler() to handle nested elasticsearch.trace Logger
    # instance in elasticsearch python client
    logging.getLogger('elasticsearch.trace').addHandler(logging.NullHandler())
    if log_opts['blacklist']:
        for bl_entry in ensure_list(log_opts['blacklist']):
            for handler in logging.root.handlers:
                handler.addFilter(Blacklist(bl_entry))

def process_config(yaml_file):
    config = test_config(yaml_file)
    set_logging(config['logging'])
    test_client_options(config['client'])
    return config['client']

def password_filter(data):
    """
    Return a deepcopy of the dictionary with any password fields hidden
    """
    def iterdict(mydict):
        for key, value in mydict.items():      
            if isinstance(value, dict):
                iterdict(value)
            elif key == "password":
                mydict.update({"password": "REDACTED"})
        return mydict
    return iterdict(deepcopy(data))
