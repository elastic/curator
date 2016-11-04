from voluptuous import *
from ..defaults import settings
from ..exceptions import ConfigurationError
from . import SchemaCheck, filtertypes
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
    retval = {
        Optional('aliases'): Any(str, [str], unicode, [unicode]),
        Optional('allocation_type'): Any(str, unicode),
        Optional('count'): Coerce(int),
        Optional('direction'): Any(str, unicode),
        Optional('disk_space'): float,
        Optional('epoch'): Any(Coerce(int), None),
        Optional('exclude'): Any(int, str, unicode, bool, None),
        Optional('field'): Any(str, unicode, None),
        Optional('key'): Any(str, unicode),
        Optional('kind'): Any(str, unicode),
        Optional('max_num_segments'): Coerce(int),
        Optional('reverse'): Any(int, str, unicode, bool, None),
        Optional('source'): Any(str, unicode),
        Optional('state'): Any(str, unicode),
        Optional('stats_result'): Any(str, unicode, None),
        Optional('timestring'): Any(str, unicode, None),
        Optional('unit'): Any(str, unicode),
        Optional('unit_count'): Coerce(int),
        Optional('use_age'): Boolean(),
        Optional('value'): Any(int, float, str, unicode, bool),
    }
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
