"""Deepfreeze Exceptions"""


class DeepfreezeException(Exception):
    """
    Base class for all exceptions raised by Deepfreeze which are not Elasticsearch
    exceptions.
    """


class MissingIndexError(DeepfreezeException):
    """
    Exception raised when a misconfiguration is detected
    """
