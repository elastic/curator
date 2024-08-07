"""Filter element schema definitions

All member functions return a :class:`voluptuous.schema_builder.Schema` object
"""

from voluptuous import All, Any, Boolean, Coerce, Optional, Range, Required
from curator.defaults import settings

# pylint: disable=unused-argument, line-too-long


def aliases(**kwargs):
    """
    :returns: {Required('aliases'): Any(list, str)}
    """
    return {Required('aliases'): Any(list, str)}


def allocation_type(**kwargs):
    """
    :returns: {Optional('allocation_type', default='require'):
        All(Any(str), Any('require', 'include', 'exclude'))}
    """
    return {
        Optional('allocation_type', default='require'): All(
            Any(str), Any('require', 'include', 'exclude')
        )
    }


def count(**kwargs):
    """
    This setting is only used with the count filtertype and is required

    :returns: {Required('count'): All(Coerce(int), Range(min=1))}
    """
    return {Required('count'): All(Coerce(int), Range(min=1))}


def date_from(**kwargs):
    """
    This setting is only used with the period filtertype.

    :returns: {Optional('date_from'): Any(str)}
    """
    return {Optional('date_from'): Any(str)}


def date_from_format(**kwargs):
    """
    This setting is only used with the period filtertype.

    :returns: {Optional('date_from_format'): Any(str)}
    """
    return {Optional('date_from_format'): Any(str)}


def date_to(**kwargs):
    """
    This setting is only used with the period filtertype.

    :returns: {Optional('date_to'): Any(str)}
    """
    return {Optional('date_to'): Any(str)}


def date_to_format(**kwargs):
    """
    This setting is only used with the period filtertype.

    :returns: {Optional('date_to_format'): Any(str)}
    """
    return {Optional('date_to_format'): Any(str)}


def direction(**kwargs):
    """
    This setting is only used with the ``age`` filtertype.

    :returns: {Required('direction'): Any('older', 'younger')}
    """
    return {Required('direction'): Any('older', 'younger')}


def disk_space(**kwargs):
    """
    This setting is only used with the ``space`` filtertype and is required

    :returns: {Required('disk_space'): Any(Coerce(float))}
    """
    return {Required('disk_space'): Any(Coerce(float))}


def epoch(**kwargs):
    """
    This setting is only used with the ``age`` filtertype.

    :returns: {Optional('epoch', default=None): Any(Coerce(int), None)}
    """

    return {Optional('epoch', default=None): Any(Coerce(int), None)}


def exclude(**kwargs):
    """
    This setting is available in all filter types.
    The default ``val`` is ``True`` if ``exclude`` in ``kwargs``, otherwise ``False``

    :returns: {Optional('exclude', default=val):
        Any(bool, All(Any(str), Boolean()))}
    """
    val = bool('exclude' in kwargs and kwargs['exclude'])
    # pylint: disable=no-value-for-parameter
    return {Optional('exclude', default=val): Any(bool, All(Any(str), Boolean()))}


def field(**kwargs):
    """
    This setting is only used with the ``age`` filtertype.

    :returns: {Required('field'): Any(str)} if ``kwargs['required']`` is ``True``
        otherwise {Optional('field'): Any(str)}
    """
    if 'required' in kwargs and kwargs['required']:
        return {Required('field'): Any(str)}
    return {Optional('field'): Any(str)}


def intersect(**kwargs):
    """
    This setting is only used with the period filtertype when using field_stats, i.e.
    indices only.

    :returns: {Optional('intersect', default=False):
        Any(bool, All(Any(str), Boolean()))}
    """
    # pylint: disable=no-value-for-parameter
    return {Optional('intersect', default=False): Any(bool, All(Any(str), Boolean()))}


def key(**kwargs):
    """
    This setting is only used with the allocated filtertype.

    :returns: {Required('key'): Any(str)}
    """
    return {Required('key'): Any(str)}


def kind(**kwargs):
    """
    This setting is only used with the pattern filtertype and is required

    :returns: {Required('kind'): Any('prefix', 'suffix', 'timestring', 'regex')}
    """
    return {Required('kind'): Any('prefix', 'suffix', 'timestring', 'regex')}


def max_num_segments(**kwargs):
    """
    :returns: {Required('max_num_segments'): All(Coerce(int), Range(min=1))}
    """
    return {Required('max_num_segments'): All(Coerce(int), Range(min=1))}


def number_of_shards(**kwargs):
    """
    :returns: {Required('number_of_shards'): All(Coerce(int), Range(min=1))}
    """
    return {Required('number_of_shards'): All(Coerce(int), Range(min=1))}


def pattern(**kwargs):
    """
    :returns: {Optional('pattern'): Any(str)}
    """
    return {Optional('pattern'): Any(str)}


def period_type(**kwargs):
    """
    This setting is only used with the period filtertype.

    :returns: {Optional('period_type', default='relative'):
        Any('relative', 'absolute')}
    """
    return {Optional('period_type', default='relative'): Any('relative', 'absolute')}


def range_from(**kwargs):
    """
    :returns: {Optional('range_from'): Coerce(int)}
    """
    return {Optional('range_from'): Coerce(int)}


def range_to(**kwargs):
    """
    :returns: {Optional('range_to'): Coerce(int)}
    """
    return {Optional('range_to'): Coerce(int)}


def reverse(**kwargs):
    """
    Only used with ``space`` filtertype. Should be ignored if ```use_age``` is True

    :returns: {Optional('reverse', default=True):
        Any(bool, All(Any(str), Boolean()))}
    """
    # pylint: disable=no-value-for-parameter
    return {Optional('reverse', default=True): Any(bool, All(Any(str), Boolean()))}


def shard_filter_behavior(**kwargs):
    """
    This setting is only used with the shards filtertype and defaults to 'greater_than'.

    :returns: {Optional('shard_filter_behavior', default='greater_than'):
        Any('greater_than', 'less_than', 'greater_than_or_equal',
        'less_than_or_equal', 'equal')}
    """
    return {
        Optional('shard_filter_behavior', default='greater_than'): Any(
            'greater_than',
            'less_than',
            'greater_than_or_equal',
            'less_than_or_equal',
            'equal',
        )
    }


def size_threshold(**kwargs):
    """
    This setting is only used with the size filtertype and is required

    :returns: {Required('size_threshold'): Any(Coerce(float))}
    """
    return {Required('size_threshold'): Any(Coerce(float))}


def size_behavior(**kwargs):
    """
    This setting is only used with the size filtertype and defaults to 'primary'.

    :returns: {Optional('size_behavior', default='primary'):
        Any('primary', 'total')}
    """
    return {Optional('size_behavior', default='primary'): Any('primary', 'total')}


def source(**kwargs):
    """
    This setting is only used with the ``age`` filtertype, or with the ``space``
    filtertype when ``use_age`` is set to True.

    :ivar valuelist: If ``kwargs['action']`` is in
        :py:func:`curator.defaults.settings.snapshot_actions`, then it is
        ``Any('name', 'creation_date')``, otherwise
        ``Any('name', 'creation_date', 'field_stats')``
    :returns: {Required('source'): valuelist} if ``kwargs['required']``, else
        {Optional('source'): valuelist}
    """
    if 'action' in kwargs and kwargs['action'] in settings.snapshot_actions():
        valuelist = Any('name', 'creation_date')
    valuelist = Any('name', 'creation_date', 'field_stats')
    if 'required' in kwargs and kwargs['required']:
        return {Required('source'): valuelist}
    return {Optional('source'): valuelist}


def state(**kwargs):
    """
    This setting is only used with the state filtertype.

    :returns: {Optional('state', default='SUCCESS'):
        Any('SUCCESS', 'PARTIAL', 'FAILED', 'IN_PROGRESS')}
    """
    return {
        Optional('state', default='SUCCESS'): Any(
            'SUCCESS', 'PARTIAL', 'FAILED', 'IN_PROGRESS'
        )
    }


def stats_result(**kwargs):
    """
    This setting is only used with the ``age`` filtertype.

    :returns: {Optional('stats_result', default='min_value'):
        Any('min_value', 'max_value')}
    """
    return {
        Optional('stats_result', default='min_value'): Any('min_value', 'max_value')
    }


def timestring(**kwargs):
    """
    This setting is only used with the ``age`` filtertype, or with the ``space``
    filtertype if ``use_age`` is set to ``True``.

    :returns: {Required('timestring'): Any(str)} if ``kwargs['required']`` else
      {Optional('timestring', default=None): Any(None, str)}
    """
    if 'required' in kwargs and kwargs['required']:
        return {Required('timestring'): Any(str)}
    return {Optional('timestring', default=None): Any(None, str)}


def threshold_behavior(**kwargs):
    """
    This setting is only used with the space and size filtertype and defaults to
    'greater_than'.

    :returns: {Optional('threshold_behavior', default='greater_than'):
        Any('greater_than', 'less_than')}
    """
    return {
        Optional('threshold_behavior', default='greater_than'): Any(
            'greater_than', 'less_than'
        )
    }


def unit(**kwargs):
    """
    This setting is only used with the ``age`` filtertype, or with the ``space``
    filtertype if ``use_age`` is set to ``True``.

    :returns: {Required('unit'): Any('seconds', 'minutes', 'hours', 'days', 'weeks',
        'months', 'years')}
    """
    return {
        Required('unit'): Any(
            'seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years'
        )
    }


def unit_count(**kwargs):
    """
    This setting is only used with the ``age`` filtertype, or with the ``space``
    filtertype if ``use_age`` is set to ``True``.

    :returns: {Required('unit_count'): Coerce(int)}
    """
    return {Required('unit_count'): Coerce(int)}


def unit_count_pattern(**kwargs):
    """
    This setting is used with the ``age`` filtertype to define whether the
    ``unit_count`` value is taken from the configuration or read from the index
    name via a regular expression

    :returns: {Optional('unit_count_pattern'): Any(str)}
    """
    return {Optional('unit_count_pattern'): Any(str)}


def use_age(**kwargs):
    """
    Use of this setting requires the additional setting, ``source``.

    :returns: {Optional('use_age', default=False):
        Any(bool, All(Any(str), Boolean()))}
    """
    # pylint: disable=no-value-for-parameter
    return {Optional('use_age', default=False): Any(bool, All(Any(str), Boolean()))}


def value(**kwargs):
    """
    This setting is only used with the ``pattern`` filtertype and is a required
    setting. There is a separate value option associated with the ``Allocation``
    action, and the ``allocated`` filtertype.

    :returns: {Required('value'): Any(str)}
    """
    return {Required('value'): Any(str)}


def week_starts_on(**kwargs):
    """
    :returns: {Optional('week_starts_on', default='sunday'):
        Any('Sunday', 'sunday', 'SUNDAY', 'Monday', 'monday', 'MONDAY', None)}
    """
    return {
        Optional('week_starts_on', default='sunday'): Any(
            'Sunday', 'sunday', 'SUNDAY', 'Monday', 'monday', 'MONDAY', None
        )
    }
