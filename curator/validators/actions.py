"""Validate root ``actions`` and individual ``action`` Schemas"""

from voluptuous import Any, In, Schema, Optional, Required
from es_client.helpers.schemacheck import SchemaCheck
from curator.defaults import settings


def root():
    """
    Return a valid :py:class:`~.voluptuous.schema_builder.Schema` definition which
    is a dictionary with ``actions`` :py:class:`~.voluptuous.schema_builder.Required`
    to be the root key with another dictionary as the value.
    """
    return Schema({Required('actions'): dict})


def valid_action():
    """
    Return a valid :py:class:`~.voluptuous.schema_builder.Schema` definition which is
    that the value of key ``action`` is
    :py:class:`~.voluptuous.schema_builder.Required` to be
    :py:class:`~.voluptuous.schema_builder.In` the value returned by
    :py:func:`~.curator.defaults.settings.all_actions`.
    """
    return {
        Required('action'): Any(
            In(settings.all_actions()),
            msg=f'action must be one of {settings.all_actions()}',
        )
    }


def structure(data, location):
    """
    Return a valid :py:class:`~.voluptuous.schema_builder.Schema` definition which
    tests ``data``, which is ostensibly an individual action dictionary. If it is a
    :py:func:`~.curator.validators.actions.valid_action`, then it will
    :py:meth:`~.voluptuous.schema_builder.Schema.update` the base
    :py:class:`~.voluptuous.schema_builder.Schema` with other options, based on the
    what the value of ``data['action']`` is.

    :param data: The configuration dictionary, or sub-dictionary, being validated
    :type data: dict
    :param location: A string to report which configuration sub-block is being tested.
    :type location: str
    :returns: A :py:class:`~.voluptuous.schema_builder.Schema` object
    """
    _ = SchemaCheck(
        data,
        Schema(valid_action(), extra=True),
        'action type',
        location,
    ).result()
    # Build a valid schema knowing that the action has already been validated
    retval = valid_action()
    retval.update({Optional('description', default='No description given'): Any(str)})
    retval.update({Optional('options', default=settings.default_options()): dict})
    action = data['action']
    if action in ['cluster_routing', 'create_index', 'rollover']:
        # The cluster_routing, create_index, and rollover actions should not
        # have a 'filters' block
        pass
    elif action == 'alias':
        # The alias action should not have a filters block, but should have
        # an add and/or remove block.
        retval.update(
            {
                Optional('add'): dict,
                Optional('remove'): dict,
            }
        )
    else:
        retval.update({Optional('filters', default=settings.default_filters()): list})
    return Schema(retval)
