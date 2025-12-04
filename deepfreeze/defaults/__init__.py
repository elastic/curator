"""
Option defaults for the standalone deepfreeze package.

This module provides voluptuous schema definitions for all deepfreeze options,
ported from curator/defaults/option_defaults.py (deepfreeze-specific options only).
"""

from datetime import datetime

from voluptuous import All, Any, Coerce, Optional, Range, Required


def Boolean():
    """
    Validate boolean-like string values.
    Accepts 'true', 'false', '1', '0', 'yes', 'no' (case-insensitive).
    """
    def validator(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ('true', '1', 'yes'):
                return True
            if value.lower() in ('false', '0', 'no'):
                return False
        raise ValueError(f"Invalid boolean value: {value}")
    return validator


# Setup options

def ilm_policy_name():
    """
    Name of the ILM policy to create or modify for deepfreeze operations.
    If the policy exists, it will be updated to use the deepfreeze repository.
    If it does not exist, a new policy will be created with a reasonable tiering strategy.
    """
    return {Required("ilm_policy_name"): Any(str)}


def index_template_name():
    """
    Name of the index template to attach the ILM policy to.
    The template will be updated to use the ILM policy.
    """
    return {Required("index_template_name"): Any(str)}


def year():
    """
    Year for deepfreeze operations.
    """
    return {Optional("year", default=datetime.today().year): Coerce(int)}


def month():
    """
    Month for deepfreeze operations.
    """
    return {
        Optional("month", default=datetime.today().month): All(
            Coerce(int), Range(min=1, max=12)
        )
    }


def repo_name_prefix():
    """
    Repository name prefix for deepfreeze.
    """
    return {Optional("repo_name_prefix", default="deepfreeze"): Any(str)}


def bucket_name_prefix():
    """
    Bucket name prefix for deepfreeze.
    """
    return {Optional("bucket_name_prefix", default="deepfreeze"): Any(str)}


def base_path_prefix():
    """
    Base path prefix for deepfreeze snapshots.
    """
    return {Optional("base_path_prefix", default="snapshots"): Any(str)}


def canned_acl():
    """
    Canned ACL for S3 objects.
    """
    return {
        Optional("canned_acl", default="private"): Any(
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "log-delivery-write",
            "bucket-owner-read",
            "bucket-owner-full-control",
        )
    }


def storage_class():
    """
    Storage class for S3 objects.
    """
    return {
        Optional("storage_class", default="intelligent_tiering"): Any(
            "standard",
            "reduced_redundancy",
            "standard_ia",
            "intelligent_tiering",
            "onezone_ia",
            "GLACIER",  # Also support uppercase for backwards compatibility
        )
    }


def provider():
    """
    Cloud provider for deepfreeze.
    """
    return {Optional("provider", default="aws"): Any("aws")}


def rotate_by():
    """
    Rotation strategy for deepfreeze.
    """
    return {Optional("rotate_by", default="path"): Any("path", "bucket")}


def style():
    """
    Naming style for deepfreeze repositories.
    """
    return {
        Optional("style", default="oneup"): Any("oneup", "date", "monthly", "weekly")
    }


# Rotate options

def keep():
    """
    Number of repositories to keep mounted.
    """
    return {Optional("keep", default=6): All(Coerce(int), Range(min=1, max=100))}


# Cleanup options

def refrozen_retention_days():
    """
    Retention period in days for refrozen thaw requests (used by cleanup command).
    """
    return {
        Optional("refrozen_retention_days", default=None): Any(
            None, All(Coerce(int), Range(min=0, max=365))
        )
    }


# Thaw options

def start_date():
    """
    Start date for thaw operation (ISO 8601 format).
    """
    return {Optional("start_date", default=None): Any(None, str)}


def end_date():
    """
    End date for thaw operation (ISO 8601 format).
    """
    return {Optional("end_date", default=None): Any(None, str)}


def sync():
    """
    Sync mode for thaw - wait for restore and mount (True) or return immediately (False).
    """
    return {Optional("sync", default=False): Any(bool, All(Any(str), Boolean()))}


def duration():
    """
    Number of days to keep objects restored from Glacier.
    """
    return {Optional("duration", default=7): All(Coerce(int), Range(min=1, max=30))}


def retrieval_tier():
    """
    AWS Glacier retrieval tier for thaw operation.
    """
    return {
        Optional("retrieval_tier", default="Standard"): Any(
            "Standard", "Expedited", "Bulk"
        )
    }


def check_status():
    """
    Thaw request ID to check status.
    """
    return {Optional("check_status", default=None): Any(None, str)}


def list_requests():
    """
    Flag to list all thaw requests.
    """
    return {
        Optional("list_requests", default=False): Any(bool, All(Any(str), Boolean()))
    }


def include_completed():
    """
    Include completed requests when listing thaw requests (default: exclude completed).
    """
    return {
        Optional("include_completed", default=False): Any(
            bool, All(Any(str), Boolean())
        )
    }


# Status options

def limit():
    """
    Number of most recent repositories to display in status.
    """
    return {
        Optional("limit", default=None): Any(
            None, All(Coerce(int), Range(min=1, max=10000))
        )
    }


def show_repos():
    """
    Show repositories section in status output.
    """
    return {Optional("show_repos", default=False): Any(bool, All(Any(str), Boolean()))}


def show_thawed():
    """
    Show thawed repositories section in status output.
    """
    return {Optional("show_thawed", default=False): Any(bool, All(Any(str), Boolean()))}


def show_buckets():
    """
    Show buckets section in status output.
    """
    return {
        Optional("show_buckets", default=False): Any(bool, All(Any(str), Boolean()))
    }


def show_ilm():
    """
    Show ILM policies section in status output.
    """
    return {Optional("show_ilm", default=False): Any(bool, All(Any(str), Boolean()))}


def show_config():
    """
    Show configuration section in status output.
    """
    return {Optional("show_config", default=False): Any(bool, All(Any(str), Boolean()))}


# Common options

def porcelain():
    """
    Output plain text without formatting (suitable for scripting).
    """
    return {Optional("porcelain", default=False): Any(bool, All(Any(str), Boolean()))}


# Refreeze options

def repo_id():
    """
    Repository name/ID to refreeze (if not provided, all thawed repos will be refrozen).
    """
    return {Optional("repo_id", default=None): Any(None, str)}


def thaw_request_id():
    """
    Thaw request ID to refreeze (if not provided, all open thaw requests will be refrozen).
    """
    return {Optional("thaw_request_id", default=None): Any(None, str)}
