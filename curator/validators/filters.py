from voluptuous import Any, In, Required, Schema
from curator.defaults import settings, filtertypes
from curator.exceptions import ConfigurationError
from curator.validators import SchemaCheck
import logging
logger = logging.getLogger(__name__)

def filtertype():
    return {
        Required('filtertype'): Any(
            In(settings.all_filtertypes()),
            msg='filtertype must be one of {0}'.format(
                settings.all_filtertypes()
            )
        )
    }

def structure():
    # This is to first ensure that only the possible keys/filter elements are
    # there, and get a dictionary back to work with.
    retval = settings.structural_filter_elements()
    retval.update(filtertype())
    return Schema(retval)

def single(action, data):
    try:
        ft = data['filtertype']
    except KeyError:
        raise ConfigurationError('Missing key "filtertype"')
    f = filtertype()
    for each in getattr(filtertypes, ft)(action, data):
        f.update(each)
    return Schema(f)

def Filters(action, location=None):
    def f(v):
        def prune_nones(mydict):
            return dict([(k,v) for k, v in mydict.items() if v != None and v != 'None'])
        # This validator method simply validates all filters in the list.
        for idx in range(0, len(v)):
            pruned = prune_nones(v[idx])
            filter_dict = SchemaCheck(
                pruned,
                single(action, pruned),
                'filter',
                '{0}, filter #{1}: {2}'.format(location, idx, pruned)
            ).result()
            logger.debug('Filter #{0}: {1}'.format(idx, filter_dict))
            v[idx] = filter_dict
        # If we've made it here without raising an Exception, it's valid
        return v
    return f
