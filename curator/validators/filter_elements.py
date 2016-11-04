from voluptuous import *
from ..defaults import settings

### Schema information ###

def aliases(**kwargs):
    # This setting is used by the alias filtertype and is required
    return { Required('aliases'): Any(str, [str], unicode, [unicode]) }

def allocation_type(**kwargs):
    return { Optional('allocation_type', default='require'): All(
        Any(str, unicode), Any('require', 'include', 'exclude')) }

def count(**kwargs):
    # This setting is only used with the count filtertype and is required
    return { Required('count'): All(Coerce(int), Range(min=1)) }

def direction(**kwargs):
    # This setting is only used with the age filtertype.
    return { Required('direction'): Any('older', 'younger') }

def disk_space(**kwargs):
    # This setting is only used with the space filtertype and is required
    return { Required('disk_space'): Any(Coerce(float)) }

def epoch(**kwargs):
    # This setting is only used with the age filtertype.
    return { Optional('epoch', default=None): Any(Coerce(int), None) }

def exclude(**kwargs):
    # This setting is available in all filter types.
    if 'exclude' in kwargs and kwargs['exclude']:
        val = True
    else: # False by default
        val = False
    return { Optional('exclude', default=val): Boolean() }

def field(**kwargs):
    # This setting is only used with the age filtertype.
    if 'required' in kwargs and kwargs['required']:
        return { Required('field'): Any(str, unicode) }
    else:
        return { Optional('field'): Any(str, unicode) }

def key(**kwargs):
    # This setting is only used with the allocated filtertype.
    return { Required('key'): Any(str, unicode) }

def kind(**kwargs):
    # This setting is only used with the pattern filtertype and is required
    return {
        Required('kind'): Any('prefix', 'suffix', 'timestring', 'regex')
    }

def max_num_segments(**kwargs):
    return {
        Required('max_num_segments'): All(Coerce(int), Range(min=1))
    }

def reverse(**kwargs):
    # Only used with space filtertype
    # Should be ignored if `use_age` is True
    return { Optional('reverse', default=True): Boolean() }

def source(**kwargs):
    # This setting is only used with the age filtertype, or with the space
    # filtertype when use_age is set to True.
    if 'action' in kwargs and kwargs['action'] in settings.snapshot_actions():
        valuelist = Any('name', 'creation_date')
    else:
        valuelist = Any('name', 'creation_date', 'field_stats')

    if 'required' in kwargs and kwargs['required']:
        return { Required('source'): valuelist }
    else:
        return { Optional('source'): valuelist }

def state(**kwargs):
    # This setting is only used with the state filtertype.
    return { Optional('state', default='SUCCESS'): Any(
        'SUCCESS', 'PARTIAL', 'FAILED', 'IN_PROGRESS') }

def stats_result(**kwargs):
    # This setting is only used with the age filtertype.
    return {
        Optional('stats_result', default='min_value'): Any(
            'min_value', 'max_value')
    }

def timestring(**kwargs):
    # This setting is only used with the age filtertype, or with the space
    # filtertype if use_age is set to True.
    if 'required' in kwargs and kwargs['required']:
        return { Required('timestring'): Any(str, unicode) }
    else:
        return { Optional('timestring', default=None): Any(str, unicode, None) }

def unit(**kwargs):
    # This setting is only used with the age filtertype, or with the space
    # filtertype if use_age is set to True.
    return {
        Required('unit'): Any(
            'seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years'
        )
    }

def unit_count(**kwargs):
    # This setting is only used with the age filtertype, or with the space
    # filtertype if use_age is set to True.
    return { Required('unit_count'): Coerce(int) }

def use_age(**kwargs):
    # Use of this setting requires the additional setting, source.
    return { Optional('use_age', default=False): Boolean() }

def value(**kwargs):
    # This setting is only used with the pattern filtertype and is a required
    # setting. There is a separate value option associated with the allocation
    # action, and the allocated filtertype.
    return { Required('value'): Any(str, unicode) }
