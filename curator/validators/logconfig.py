"""Logging Schema definition"""
from voluptuous import Optional, Schema
from curator.defaults import logging_defaults

def logging():
    return Schema(
        {
            Optional('logging'): logging_defaults.config_logging(),
        }
    )
