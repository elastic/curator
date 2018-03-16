class CuratorException(Exception):
    """
    Base class for all exceptions raised by Curator which are not Elasticsearch
    exceptions.
    """

class ConfigurationError(CuratorException):
    """
    Exception raised when a misconfiguration is detected
    """

class MissingArgument(CuratorException):
    """
    Exception raised when a needed argument is not passed.
    """

class NoIndices(CuratorException):
    """
    Exception raised when an operation is attempted against an empty index_list
    """

class NoSnapshots(CuratorException):
    """
    Exception raised when an operation is attempted against an empty snapshot_list
    """

class ActionError(CuratorException):
    """
    Exception raised when an action (against an index_list or snapshot_list) cannot be taken.
    """

class FailedExecution(CuratorException):
    """
    Exception raised when an action fails to execute for some reason.
    """

class SnapshotInProgress(ActionError):
    """
    Exception raised when a snapshot is already in progress
    """

class ActionTimeout(CuratorException):
    """
    Exception raised when an action fails to complete in the allotted time
    """
