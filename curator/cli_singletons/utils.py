"""Singleton Utils Module"""
import json
from click import BadParameter
from es_client.helpers.utils import ensure_list

# Click functions require ctx and param to be passed positionally even if not used
# pylint: disable=unused-argument
def json_to_dict(ctx, param, value):
    """Convert JSON to dictionary"""
    if value is None:
        return {}
    try:
        return json.loads(value)
    except ValueError as exc:
        raise BadParameter(f'Invalid JSON for {param}: {value}') from exc

# Click functions require ctx and param to be passed positionally even if not used
# pylint: disable=unused-argument
def validate_filter_json(ctx, param, value):
    """Validate the JSON provided from the command-line"""
    # The "None" edge case should only be for optional filters, like alias add/remove
    if value is None:
        return value
    try:
        filter_list = ensure_list(json.loads(value))
        return filter_list
    except ValueError as exc:
        raise BadParameter(f'Filter list is invalid JSON: {value}') from exc
