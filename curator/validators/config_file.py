from voluptuous import Optional, Schema
from curator.defaults import client_defaults

def client():
    return Schema(
        {
            Optional('client'): client_defaults.config_client(),
            Optional('logging'): client_defaults.config_logging(),
        }
    )
