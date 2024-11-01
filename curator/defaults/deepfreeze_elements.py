"""Deepfreeze element schema definitions

All member functions return a :class:`voluptuous.schema_builder.Schema` object
"""

from voluptuous import All, Any, Coerce, Optional, Range, Required

# pylint: disable=unused-argument, line-too-long


def repo_name_prefix():
    """
    Return a :class:`voluptuous.schema_builder.Schema` object for `repo_name_prefix`
    """
    return {Optional("repo_name_prefix"): All(Any(str), default="deepfreeze-")}


def bucket_name_prefix():
    """
    Return a :class:`voluptuous.schema_builder.Schema` object for `bucket_name_prefix`
    """
    return {Optional("bucket_name_prefix"): All(Any(str), default="deepfreeze-")}


def base_path():
    """
    Return a :class:`voluptuous.schema_builder.Schema` object for `base_path`
    """
    return {Optional("base_path"): All(Any(str), default="snapshots")}


def canned_acl():
    """
    Return a :class:`voluptuous.schema_builder.Schema` object for `canned_acl`
    """
    return {
        Optional("canned_acl"): All(
            Any(
                "private",
                "public-read",
                "public-read-write",
                "authenticated-read",
                "log-delivery-write",
                "bucket-owner-read",
                "bucket-owner-full-control",
            ),
            default="private",
        )
    }


def storage_class():
    """
    Return a :class:`voluptuous.schema_builder.Schema` object for `storage_class`
    """
    return {
        Optional("storage_class"): All(
            Any(
                "standard",
                "reduced_redundancy",
                "standard_ia",
                "intelligent_tiering",
                "onezone_ia",
            ),
            default="intelligent_tiering",
        )
    }


def keep():
    """
    This setting is required.
    Return a :class:`voluptuous.schema_builder.Schema` object for `keep`
    """
    return {Required("keep"): All(Coerce(int), Range(min=1))}


def year():
    """
    This setting is only used to override the current year value.
    Return a :class:`voluptuous.schema_builder.Schema` object for `year`
    """
    return {Optional("year"): All(Coerce(int), Range(min=2000, max=2100))}


def month():
    """
    This setting is only used to override the current month value.
    Return a :class:`voluptuous.schema_builder.Schema` object for `month`
    """
    return {Optional("month"): All(Coerce(int), Range(min=1, max=12))}

def setup():
    """
    This setting should be used once, to initialize a deepfreeze repository 
    and bucket.
    Return a :class:`voluptuous.schema_builder.Schema` object for `setup`
    """
    return {Optional("setup"): Any(bool, default=False)}