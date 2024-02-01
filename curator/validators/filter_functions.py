"""Functions validating the ``filter`` Schema of an ``action``"""
import logging
from voluptuous import Any, In, Required, Schema
from es_client.helpers.schemacheck import SchemaCheck
from es_client.helpers.utils import prune_nones
from curator.defaults import settings, filtertypes
from curator.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

def filtertype():
    """
    Return a :py:class:`~.voluptuous.schema_builder.Schema` object that uses
    :py:func:`~.curator.defaults.settings.all_filtertypes` to populate acceptable values

    :returns: A :py:class:`~.voluptuous.schema_builder.Schema` object
    """
    return {
        Required('filtertype'): Any(
            In(settings.all_filtertypes()),
            msg=f'filtertype must be one of {settings.all_filtertypes()}'
        )
    }

def filterstructure():
    """
    Return a :py:class:`~.voluptuous.schema_builder.Schema` object that uses the return value from
    :py:func:`~.curator.defaults.settings.structural_filter_elements` to populate acceptable values
    and updates/merges the Schema object with the return value from
    :py:func:`filtertype`

    :returns: A :py:class:`~.voluptuous.schema_builder.Schema` object
    """
    # This is to first ensure that only the possible keys/filter elements are
    # there, and get a dictionary back to work with.
    retval = settings.structural_filter_elements()
    retval.update(filtertype())
    return Schema(retval)

def singlefilter(action, data):
    """
    Return a :py:class:`~.voluptuous.schema_builder.Schema` object that is created using the return
    value from :py:func:`filtertype` to create a local variable ``ftype``. The values from
    ``action`` and ``data`` are used to update ``ftype`` based on matching function names in
    :py:mod:`~.curator.defaults.filtertypes`.

    :py:func:`~.curator.defaults.settings.structural_filter_elements` to populate acceptable values
    and updates/merges the Schema object with the return value from
    :py:func:`filtertype`

    :param action: The Curator action name
    :type action: str
    :param data: The filter block of the action

    :returns: A :py:class:`~.voluptuous.schema_builder.Schema` object
    """
    try:
        ftdata = data['filtertype']
    except KeyError as exc:
        raise ConfigurationError('Missing key "filtertype"') from exc
    ftype = filtertype()
    for each in getattr(filtertypes, ftdata)(action, data):
        ftype.update(each)
    return Schema(ftype)

def validfilters(action, location=None):
    """Validate the filters in a list"""
    def func(val):
        """This validator method simply validates all filters in the list."""
        for idx, value in enumerate(val):
            pruned = prune_nones(value)
            filter_dict = SchemaCheck(
                pruned,
                singlefilter(action, pruned),
                'filter',
                f'{location}, filter #{idx}: {pruned}'
            ).result()
            logger.debug('Filter #%s: %s', idx, filter_dict)
            val[idx] = filter_dict
        # If we've made it here without raising an Exception, it's valid
        return val
    return func
