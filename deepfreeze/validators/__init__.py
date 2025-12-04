"""
Schema validation for the standalone deepfreeze package.

This module provides voluptuous schemas for validating deepfreeze options,
ported from curator/validators/options.py (deepfreeze-specific schemas only).
"""

from voluptuous import Schema

from deepfreeze import defaults


# Deepfreeze action option schemas
# Each schema lists the option defaults that apply to that action

DEEPFREEZE_OPTIONS = {
    'setup': [
        defaults.year(),
        defaults.month(),
        defaults.repo_name_prefix(),
        defaults.bucket_name_prefix(),
        defaults.base_path_prefix(),
        defaults.canned_acl(),
        defaults.storage_class(),
        defaults.provider(),
        defaults.rotate_by(),
        defaults.style(),
        defaults.ilm_policy_name(),
        defaults.index_template_name(),
        defaults.porcelain(),
    ],
    'rotate': [
        defaults.keep(),
        defaults.year(),
        defaults.month(),
        defaults.porcelain(),
    ],
    'cleanup': [
        defaults.refrozen_retention_days(),
        defaults.porcelain(),
    ],
    'status': [
        defaults.limit(),
        defaults.show_repos(),
        defaults.show_thawed(),
        defaults.show_buckets(),
        defaults.show_ilm(),
        defaults.show_config(),
        defaults.porcelain(),
    ],
    'thaw': [
        defaults.start_date(),
        defaults.end_date(),
        defaults.sync(),
        defaults.duration(),
        defaults.retrieval_tier(),
        defaults.check_status(),
        defaults.list_requests(),
        defaults.include_completed(),
        defaults.porcelain(),
    ],
    'refreeze': [
        defaults.thaw_request_id(),
        defaults.porcelain(),
    ],
    'repair_metadata': [
        defaults.porcelain(),
    ],
}


def _build_schema(option_list: list) -> Schema:
    """
    Build a voluptuous Schema from a list of option definitions.

    Each option definition is a dict with a single key (the option name)
    and a validation rule as the value.

    Args:
        option_list: List of option definition dicts

    Returns:
        Schema: A voluptuous Schema that validates all options
    """
    schema_dict = {}
    for option_def in option_list:
        schema_dict.update(option_def)
    return Schema(schema_dict)


# Pre-built schemas for each action
SETUP_SCHEMA = _build_schema(DEEPFREEZE_OPTIONS['setup'])
ROTATE_SCHEMA = _build_schema(DEEPFREEZE_OPTIONS['rotate'])
CLEANUP_SCHEMA = _build_schema(DEEPFREEZE_OPTIONS['cleanup'])
STATUS_SCHEMA = _build_schema(DEEPFREEZE_OPTIONS['status'])
THAW_SCHEMA = _build_schema(DEEPFREEZE_OPTIONS['thaw'])
REFREEZE_SCHEMA = _build_schema(DEEPFREEZE_OPTIONS['refreeze'])
REPAIR_METADATA_SCHEMA = _build_schema(DEEPFREEZE_OPTIONS['repair_metadata'])


# Map action names to their schemas
ACTION_SCHEMAS = {
    'setup': SETUP_SCHEMA,
    'rotate': ROTATE_SCHEMA,
    'cleanup': CLEANUP_SCHEMA,
    'status': STATUS_SCHEMA,
    'thaw': THAW_SCHEMA,
    'refreeze': REFREEZE_SCHEMA,
    'repair_metadata': REPAIR_METADATA_SCHEMA,
}


def validate_options(action: str, options: dict) -> dict:
    """
    Validate options for a given action.

    This function validates the provided options against the schema for the
    specified action, applying defaults where appropriate.

    Args:
        action: The name of the deepfreeze action (setup, rotate, status, etc.)
        options: Dictionary of option values to validate

    Returns:
        dict: Validated and normalized options with defaults applied

    Raises:
        voluptuous.Invalid: If validation fails
        KeyError: If the action is not recognized
    """
    if action not in ACTION_SCHEMAS:
        raise KeyError(f"Unknown action: {action}. Valid actions are: {list(ACTION_SCHEMAS.keys())}")

    schema = ACTION_SCHEMAS[action]
    return schema(options)


def get_schema(action: str) -> Schema:
    """
    Get the validation schema for a given action.

    Args:
        action: The name of the deepfreeze action

    Returns:
        Schema: The voluptuous Schema for the action

    Raises:
        KeyError: If the action is not recognized
    """
    if action not in ACTION_SCHEMAS:
        raise KeyError(f"Unknown action: {action}. Valid actions are: {list(ACTION_SCHEMAS.keys())}")
    return ACTION_SCHEMAS[action]


def get_default_options(action: str) -> dict:
    """
    Get the default options for a given action.

    This function runs validation with an empty dict to get all defaults.

    Args:
        action: The name of the deepfreeze action

    Returns:
        dict: Default options for the action

    Raises:
        KeyError: If the action is not recognized
        voluptuous.Invalid: If required options cannot be defaulted
    """
    # For actions with required fields, we can't get defaults without values
    # In such cases, this will raise an error which is expected
    schema = get_schema(action)
    try:
        return schema({})
    except Exception:
        # If there are required fields, return what we can
        return {}
