"""Deepfreeze Exceptions"""


class DeepfreezeException(Exception):
    """
    Base class for all exceptions raised by Deepfreeze which are not Elasticsearch
    exceptions.
    """


class MissingIndexError(DeepfreezeException):
    """
    Exception raised when the status index is missing
    """


class MissingSettingsError(DeepfreezeException):
    """
    Exception raised when the status index exists, but the settings document is missing
    """


class ActionException(DeepfreezeException):
    """
    Generic class for unexpected coneditions during DF actions
    """


class PreconditionError(DeepfreezeException):
    """
    Exception raised when preconditions are not met for a deepfreeze action
    """


class RepositoryException(DeepfreezeException):
    """
    Exception raised when a probley with a repository occurs
    """
