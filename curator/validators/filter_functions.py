"""Filter functions"""
import logging
from voluptuous import Any, In, Required, Schema
from es_client.helpers.utils import prune_nones
from curator.defaults import settings, filtertypes
from curator.exceptions import ConfigurationError
from curator.validators import SchemaCheck

logger = logging.getLogger(__name__)

def filtertype():
    """Define a filtertype"""
    return {
        Required('filtertype'): Any(
            In(settings.all_filtertypes()),
            msg=f'filtertype must be one of {settings.all_filtertypes()}'
        )
    }

def filterstructure():
    """Extract structural elements from filter"""
    # This is to first ensure that only the possible keys/filter elements are
    # there, and get a dictionary back to work with.
    retval = settings.structural_filter_elements()
    retval.update(filtertype())
    return Schema(retval)

def singlefilter(action, data):
    """Get the filter Schema for a single filtertype"""
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
