
from datetime import timedelta, datetime, date
import time
import re
import logging
from .exceptions import *
from .settings import *
from .utils import *


class IndexList(object):
    def __init__(self, client):
        verify_client_object(client)
        self.loggit = logging.getLogger('curator.indexlist')
        #: An Elasticsearch Client object
        #: Also accessible as an instance variable.
        self.client = client
        #: Instance variable.
        #: Information extracted from indices, such as segment count, age, etc.
        #: Populated at instance creation time, and by other private helper
        #: methods, as needed. **Type:** ``dict()``
        self.index_info = {}
        #: Instance variable.
        #: The running list of indices which will be used by an Action class.
        #: Populated at instance creation time. **Type:** ``list()``
        self.indices = []
        #: Instance variable.
        #: All indices in the cluster at instance creation time.
        #: **Type:** ``list()``
        self.all_indices = []
        self.__get_indices()

    def __actionable(self, idx):
        self.loggit.info(
            'Index {0} is actionable and remains in the list.'.format(idx))

    def __not_actionable(self, idx):
            self.loggit.info(
                'Index {0} is not actionable, removing from list.'.format(idx))
            self.indices.remove(idx)

    def __excludify(self, condition, exclude, index, msg=None):
        if condition == True:
            if exclude:
                text = "Removed from actionable list"
                self.__not_actionable(index)
            else:
                text = "Remains in actionable list"
                self.__actionable(index)
        else:
            if exclude:
                text = "Remains in actionable list"
                self.__actionable(index)
            else:
                text = "Removed from actionable list"
                self.__not_actionable(index)
        if msg:
            self.loggit.info('{0}: {1}'.format(text, msg))

    def __get_indices(self):
        """
        Pull all indices into `all_indices`, then populate `indices` and
        `index_info`
        """
        self.loggit.debug('Getting all indices')
        self.all_indices = get_indices(self.client)
        self.indices = self.all_indices
        self.empty_list_check()
        for index in self.indices:
            self.__build_index_info(index)
        self._get_metadata()
        self._get_index_stats()

    def __build_index_info(self, index):
        """
        Ensure that `index` is a key in `index_info`. If not, create a
        sub-dictionary structure under that key.
        """
        self.loggit.debug('Building index info dictionary')
        if not index in self.index_info:
            self.index_info[index] = {
                "age" : {},
                "number_of_replicas" : 0,
                "number_of_shards" : 0,
                "segments" : 0,
                "size_in_bytes" : 0,
                "docs" : 0,
                "state" : "",
            }

    def __map_method(self, ft):
        methods = {
            'age': self.filter_by_age,
            'allocated': self.filter_allocated,
            'closed': self.filter_closed,
            'forcemerged': self.filter_forceMerged,
            'kibana': self.filter_kibana,
            'none': self.filter_none,
            'opened': self.filter_opened,
            'pattern': self.filter_by_regex,
            'space': self.filter_by_space,
        }
        return methods[ft]

    def _get_index_stats(self):
        """
        Populate `index_info` with index `size_in_bytes` and doc count
        information for each index.
        """
        self.loggit.debug('Getting index stats')
        self.empty_list_check()
        # Subroutine to do the dirty work
        def iterate_over_stats(stats):
            for index in stats['indices']:
                size = stats['indices'][index]['total']['store']['size_in_bytes']
                docs = stats['indices'][index]['total']['docs']['count']
                self.loggit.debug(
                    'Index: {0}  Size: {1}  Docs: {2}'.format(
                        index, byte_size(size), docs
                    )
                )
                self.index_info[index]['size_in_bytes'] = size
                self.index_info[index]['docs'] = docs

        working_list = self.working_list()
        for index in self.working_list():
            if self.index_info[index]['state'] == 'close':
                working_list.remove(index)
        if working_list:
            index_lists = chunk_index_list(working_list)
            for l in index_lists:
                iterate_over_stats(
                    self.client.indices.stats(index=to_csv(l),
                    metric='store,docs')
                )

    def _get_metadata(self):
        """
        Populate `index_info` with index `size_in_bytes` and doc count
        information for each index.
        """
        self.loggit.debug('Getting index metadata')
        self.empty_list_check()
        index_lists = chunk_index_list(self.indices)
        for l in index_lists:
            working_list = (
                self.client.cluster.state(
                    index=to_csv(l),metric='metadata'
                )['metadata']['indices']
            )
            if working_list:
                for index in list(working_list.keys()):
                    s = self.index_info[index]
                    wl = working_list[index]
                    s['age']['creation_date'] = (
                        fix_epoch(wl['settings']['index']['creation_date'])
                    )
                    s['number_of_replicas'] = (
                        wl['settings']['index']['number_of_replicas']
                    )
                    s['number_of_shards'] = (
                        wl['settings']['index']['number_of_shards']
                    )
                    s['state'] = wl['state']
                    if 'routing' in wl['settings']['index']:
                        s['routing'] = wl['settings']['index']['routing']

    def empty_list_check(self):
        """Raise exception if `indices` is empty"""
        self.loggit.debug('Checking for empty list')
        if not self.indices:
            raise NoIndices('index_list object is empty.')

    def working_list(self):
        """
        Return the current value of `indices` as copy-by-value to prevent list
        stomping during iterations
        """
        # Copy by value, rather than reference to prevent list stomping during
        # iterations
        self.loggit.debug('Generating working list of indices')
        return self.indices[:]

    def _get_segmentcounts(self):
        """
        Populate `index_info` with segment information for each index.
        """
        self.loggit.debug('Getting index segment counts')
        self.empty_list_check()
        index_lists = chunk_index_list(self.indices)
        for l in index_lists:
            working_list = (
                self.client.indices.segments(index=to_csv(l))['indices']
            )
            if working_list:
                for index in list(working_list.keys()):
                    shards = working_list[index]['shards']
                    segmentcount = 0
                    for shardnum in shards:
                        for shard in range(0,len(shards[shardnum])):
                            segmentcount += (
                                shards[shardnum][shard]['num_search_segments']
                            )
                    self.index_info[index]['segments'] = segmentcount

    def _get_name_based_ages(self, timestring):
        """
        Add indices to `index_info` based on the age as indicated by the index
        name pattern, if it matches `timestring`

        :arg timestring: An strftime pattern
        """
        # Check for empty list before proceeding here to prevent non-iterable
        # condition
        self.loggit.debug('Getting ages of indices by "name"')
        self.empty_list_check()
        ts = TimestringSearch(timestring)
        for index in self.working_list():
            epoch = ts.get_epoch(index)
            if epoch:
                self.index_info[index]['age']['name'] = epoch
                self.loggit.debug(
                    'Index {0} translates to epoch timestamp: '
                    '{1}'.format(index, int(epoch))
                )

    def _get_field_stats_dates(self, field='@timestamp'):
        """
        Add indices to `index_info` based on the value the stats api returns,
        as determined by `field`

        :arg field: The field with the date value.  The field must be mapped in
            elasticsearch as a date datatype.  Default: ``@timestamp``
        """
        self.loggit.debug('Getting index date from field_stats API')
        self.loggit.info(
            'Cannot use field_stats on closed indices.  '
            'Omitting any closed indices.'
        )
        self.filter_closed()
        index_lists = chunk_index_list(self.indices)
        for l in index_lists:
            working_list = self.client.field_stats(
                index=to_csv(l), fields=field, level='indices'
                )['indices']
            if working_list:
                for index in list(working_list.keys()):
                    try:
                        s = self.index_info[index]['age']
                        wl = working_list[index]['fields'][field]
                        # Use these new references to keep these lines more
                        # readable
                        s['min_value'] = fix_epoch(wl['min_value'])
                        s['max_value'] = fix_epoch(wl['max_value'])
                    except KeyError as e:
                        raise ActionError(
                            'Field "{0}" not found in index '
                            '"{1}"'.format(field, index)
                        )

    def filter_by_regex(self, kind=None, value=None, exclude=False):
        """
        Match indices by regular expression (pattern).

        :arg kind: Can be one of: ``suffix``, ``prefix``, ``regex``, or
            ``timestring``. This option defines what kind of filter you will be
            building.
        :arg value: Depends on `kind`. It is the strftime string if `kind` is
            ``timestring``. It's used to build the regular expression for other
            kinds.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `False`
        """
        self.loggit.debug('Filtering indices by regex')
        if kind not in [ 'regex', 'prefix', 'suffix', 'timestring' ]:
            raise ValueError('{0}: Invalid value for kind'.format(kind))

        # Stop here if None or empty value, but zero is okay
        if value == 0:
            pass
        elif not value:
            raise ValueError(
                '{0}: Invalid value for "value". '
                'Cannot be "None" type, empty, or False'
            )

        if kind == 'timestring':
            regex = REGEX_MAP[kind].format(get_date_regex(value))
        else:
            regex = REGEX_MAP[kind].format(value)

        self.empty_list_check()
        pattern = re.compile(regex)
        for index in self.working_list():
            self.loggit.debug('Filter by regex: Index: {0}'.format(index))
            match = pattern.match(index)
            if match:
                self.__excludify(True, exclude, index)
            else:
                self.__excludify(False, exclude, index)

    def filter_by_age(self, source='name', direction=None, timestring=None,
        unit=None, unit_count=None, field=None, stats_result='min_value',
        epoch=None, exclude=False,
        ):
        """
        Match `indices` by relative age calculations.

        :arg source: Source of index age. Can be one of 'name', 'creation_date',
            or 'field_stats'
        :arg direction: Time to filter, either ``older`` or ``younger``
        :arg timestring: An strftime string to match the datestamp in an index
            name. Only used for index filtering by ``name``.
        :arg unit: One of ``seconds``, ``minutes``, ``hours``, ``days``,
            ``weeks``, ``months``, or ``years``.
        :arg unit_count: The number of ``unit``s. ``unit_count`` * ``unit`` will
            be calculated out to the relative number of seconds.
        :arg field: A timestamp field name.  Only used for ``field_stats`` based
            calculations.
        :arg stats_result: Either `min_value` or `max_value`.  Only used in
            conjunction with `source`=``field_stats`` to choose whether to
            reference the minimum or maximum result value.
        :arg epoch: An epoch timestamp used in conjunction with ``unit`` and
            ``unit_count`` to establish a point of reference for calculations.
            If not provided, the current time will be used.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `False`
        """

        self.loggit.debug('Filtering indices by age')
        # Get timestamp point of reference, PoR
        PoR = get_point_of_reference(unit, unit_count, epoch)
        keyfield = source
        if not direction:
            raise MissingArgument('Must provide a value for "direction"')
        if direction not in ['older', 'younger']:
            raise ValueError(
                'Invalid value for "direction": {0}'.format(direction)
            )
        if source == 'name':
            if not timestring:
                raise MissingArgument(
                    'source "name" requires the "timestamp" keyword argument'
                )
            self._get_name_based_ages(timestring)
        elif source == 'creation_date':
            # Nothing to do here as this comes from `get_metadata` in __init__
            pass
        elif source == 'field_stats':
            if not field:
                raise MissingArgument(
                    'source "field_stats" requires the "field" keyword argument'
                )
            if stats_result not in ['min_value', 'max_value']:
                raise ValueError(
                    'Invalid value for "stats_result": {0}'.format(stats_result)
                )
            keyfield = stats_result
            self._get_field_stats_dates(field=field)
        else:
            raise ValueError(
                'Invalid source: {0}.  '
                'Must be one of "name", '
                '"creation_date", "field_stats".'.format(source)
            )

        for index in self.working_list():

            try:
                msg = (
                    'Index "{0}" age ({1}), direction: "{2}", point of '
                    'reference, ({3})'.format(
                        index,
                        int(self.index_info[index]['age'][keyfield]),
                        direction,
                        PoR
                    )
                )
                # Because time adds to epoch, smaller numbers are actually older
                # timestamps.
                if direction == 'older':
                    agetest = self.index_info[index]['age'][keyfield] < PoR
                else:
                    agetest = self.index_info[index]['age'][keyfield] > PoR
                self.__excludify(agetest, exclude, index, msg)
            except KeyError:
                self.loggit.info(
                    'Index "{0}" does not meet provided criteria. '
                    'Removing from list.'.format(index, source))
                self.indices.remove(index)

    def filter_by_space(
        self, disk_space=None, reverse=True, use_age=False,
        source='creation_date', timestring=None, field=None,
        stats_result='min_value', exclude=False):
        """
        Remove indices from the actionable list based on space
        consumed, sorted reverse-alphabetically by default.  If you set
        `reverse` to `False`, it will be sorted alphabetically.

        The default is usually what you will want. If only one kind of index is
        provided--for example, indices matching ``logstash-%Y.%m.%d``--then
        reverse alphabetical sorting will mean the oldest will remain in the
        list, because lower numbers in the dates mean older indices.

        By setting `reverse` to `False`, then ``index3`` will be deleted before
        ``index2``, which will be deleted before ``index1``

        `use_age` allows ordering indices by age. Age is determined by the index
        creation date by default, but you can specify an `source` of ``name``,
        ``max_value``, or ``min_value``.  The ``name`` `source` requires the
        timestring argument.

        :arg disk_space: Filter indices over *n* gigabytes
        :arg reverse: The filtering direction. (default: `True`).  Ignored if
            `use_age` is `True`
        :arg use_age: Sort indices by age.  ``source`` is required in this
            case.
        :arg source: Source of index age. Can be one of ``name``,
            ``creation_date``, or ``field_stats``. Default: ``creation_date``
        :arg timestring: An strftime string to match the datestamp in an index
            name. Only used if `source` ``name`` is selected.
        :arg field: A timestamp field name.  Only used if `source`
            ``field_stats`` is selected.
        :arg stats_result: Either `min_value` or `max_value`.  Only used if
            `source` ``field_stats`` is selected. It determines whether to
            reference the minimum or maximum value of `field` in each index.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `False`
        """
        self.loggit.debug('Filtering indices by disk space')
        # Ensure that disk_space is a float
        if not disk_space:
            raise MissingArgument('No value for "disk_space" provided')

        disk_space = float(disk_space)

        disk_usage = 0.0
        disk_limit = disk_space * 2**30

        self.loggit.info(
            'Cannot get disk usage info from closed indices.  '
            'Omitting any closed indices.'
        )
        self.filter_closed()

        # Create a copy-by-value working list
        working_list = self.working_list()

        if use_age:
            keyfield = source
            if source not in ['creation_date', 'name', 'field_stats']:
                raise ValueError(
                    'Invalid value for "source": {0}'.format(source)
                )
            if source == 'field_stats':
                if not field:
                    raise MissingArgument(
                        'No value for "field" provided. "field" is required '
                        'with source "field_stats"'
                    )
                if stats_result not in ['min_value', 'max_value']:
                    raise ConfigurationError(
                        'Incorrect value for "stats_result" provided: {0}. '
                        'Must be either "min_value" or '
                        '"max_value"'.format(stats_result)
                    )
                keyfield = stats_result
                self._get_field_stats_dates(field=field)
            if source == 'name':
                if not timestring:
                    raise MissingArgument(
                        'No value for "timestring" provided. "timestring" is '
                        'required with source "name"'
                    )
                self._get_name_based_ages(timestring)

            # Do the age-based sorting here.
            # First, build an intermediate dictionary with just index and age
            # as the key and value, respectively
            intermediate = {}
            for index in working_list:
                intermediate[index] = self.index_info[index]['age'][keyfield]

            # This will sort the indices the youngest first. Effectively, this
            # should set us up to delete everything older than fits into
            # `disk_space`.  It starts as a tuple, but then becomes a list.
            sorted_tuple = (
                sorted(intermediate.items(), key=lambda k: k[1], reverse=True)
            )
            sorted_indices = [x[0] for x in sorted_tuple]

        else:
            # Default to sorting by index name
            sorted_indices = sorted(working_list, reverse=reverse)

        for index in sorted_indices:

            disk_usage += self.index_info[index]['size_in_bytes']
            msg = (
                '{0}, summed disk usage is {1} and disk limit is {2}.'.format(
                    index, byte_size(disk_usage), byte_size(disk_limit)
                )
            )
            self.__excludify((disk_usage > disk_limit), exclude, index, msg)
            # if disk_usage > disk_limit:
            #     if exclude:
            #         text = "Removed from actionable list"
            #         self.__not_actionable(index)
            #     else:
            #         text = "Remains in actionable list"
            #         self.__actionable(index)
            # else:
            #     if exclude:
            #         text = "Remains in actionable list"
            #         self.__actionable(index)
            #     else:
            #         text = "Removed from actionable list"
            #         self.__not_actionable(index)
            # self.loggit.info('{0}: {1}'.format(text, msg))

    def filter_kibana(self, exclude=True):
        """
        Match any index named ``.kibana``, ``kibana-int``, ``.marvel-kibana``,
        or ``.marvel-es-data`` in `indices`.

        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `True`
        """
        self.loggit.debug('Filtering kibana indices')
        self.empty_list_check()
        for index in self.working_list():
            if index in [
                    '.kibana', '.marvel-kibana', 'kibana-int', '.marvel-es-data'
                ]:
                self.__excludify(True, exclude, index)

    def filter_forceMerged(self, max_num_segments=None, exclude=True):
        """
        Match any index which has `max_num_segments` per shard or fewer in the
        actionable list.

        :arg max_num_segments: Cutoff number of segments per shard.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `True`
        """
        self.loggit.debug('Filtering forceMerged indices')
        if not max_num_segments:
            raise MissingArgument('Missing value for "max_num_segments"')
        self.loggit.info(
            'Cannot get segment count of closed indices.  '
            'Omitting any closed indices.'
        )
        self.filter_closed()
        self._get_segmentcounts()
        for index in self.working_list():
            # Do this to reduce long lines and make it more readable...
            shards = int(self.index_info[index]['number_of_shards'])
            replicas = int(self.index_info[index]['number_of_replicas'])
            segments = int(self.index_info[index]['segments'])
            msg = (
                '{0} has {1} shard(s) + {2} replica(s) '
                'with a sum total of {3} segments.'.format(
                    index, shards, replicas, segments
                )
            )
            expected_count = ((shards + (shards * replicas)) * max_num_segments)
            self.__excludify((segments <= expected_count), exclude, index, msg)


    def filter_closed(self, exclude=True):
        """
        Filter out closed indices from `indices`

        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `True`
        """
        self.loggit.debug('Filtering closed indices')
        self.empty_list_check()
        for index in self.working_list():
            condition = self.index_info[index]['state'] == 'close'
            self.loggit.debug('Index {0} state: {1}'.format(
                    index, self.index_info[index]['state']
                )
            )
            self.__excludify(condition, exclude, index)

    def filter_opened(self, exclude=True):
        """
        Filter out opened indices from `indices`

        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `True`
        """
        self.loggit.debug('Filtering open indices')
        self.empty_list_check()
        for index in self.working_list():
            condition = self.index_info[index]['state'] == 'open'
            self.loggit.debug('Index {0} state: {1}'.format(
                    index, self.index_info[index]['state']
                )
            )
            self.__excludify(condition, exclude, index)

    def filter_allocated(self,
            key=None, value=None, allocation_type='require', exclude=True,
        ):
        """
        Match indices that have the routing allocation rule of
        `key=value` from `indices`

        :arg key: The allocation attribute to check for
        :arg value: The value to check for
        :arg allocation_type: Type of allocation to apply
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `True`
        """
        self.loggit.debug(
            'Filtering indices with shard routing allocation rules')
        if not key:
            raise MissingArgument('No value for "key" provided')
        if not value:
            raise MissingArgument('No value for "value" provided')
        if not allocation_type in ['include', 'exclude', 'require']:
            raise ValueError(
                'Invalid "allocation_type": {0}'.format(allocation_type)
            )
        self.empty_list_check()
        index_lists = chunk_index_list(self.indices)
        for l in index_lists:
            working_list = self.client.indices.get_settings(index=to_csv(l))
            if working_list:
                for index in list(working_list.keys()):
                    try:
                        has_routing = (
                            working_list[index]['settings']['index']['routing']['allocation'][allocation_type][key] == value
                        )
                    except KeyError:
                        has_routing = False
                    # if has_routing:
                    msg = (
                        '{0}: Routing (mis)match: '
                        'index.routing.allocation.{1}.{2}={3}.'.format(
                            index, allocation_type, key, value
                        )
                    )
                        # self.indices.remove(index)
                    self.__excludify(has_routing, exclude, index, msg)

    def filter_none(self):
        self.loggit.info('"None" filter selected.  No filtering will be done.')

    def iterate_filters(self, filter_dict):
        """
        Iterate over the filters defined in `config` and execute them.

        :arg filter_dict: The configuration dictionary

        .. note:: `filter_dict` should be a dictionary with the following form:
        .. code-block:: python

                { 'filters' : [
                        {
                            'filtertype': 'the_filter_type',
                            'key1' : 'value1',
                            ...
                            'keyN' : 'valueN'
                        }
                    ]
                }

        """
        self.loggit.debug('Iterating over a list of filters')
        # Make sure we actually _have_ filters to act on
        if not 'filters' in filter_dict or len(filter_dict['filters']) < 1:
            logger.info('No filters in config.  Returning unaltered object.')
            return

        self.loggit.debug('All filters: {0}'.format(filter_dict['filters']))
        for f in filter_dict['filters']:
            self.loggit.debug('Top of the loop: {0}'.format(self.indices))
            self.loggit.debug('Un-parsed filter args: {0}'.format(f))
            f_args = None
            # Make sure we got at least this much in the configuration
            if not 'filtertype' in f:
                raise ConfigurationError(
                    'No "filtertype" in filter definition.'
                )
            try:
                ft = f['filtertype'].lower()
            except Exception as e:
                raise ValueError(
                    'Invalid value for "filtertype": '
                    '{0}'.format(f['filtertype'])
                )
            try:
                f_args = IDX_FILTER_DEFAULTS[ft]
                method = self.__map_method(ft)
            except:
                raise ConfigurationError(
                    'Unrecognized filtertype: {0}'.format(ft))
            # Remove key 'filtertype' from dictionary 'f'
            del f['filtertype']
            # If it's a filtertype with arguments, update the defaults with the
            # provided settings.
            if f_args:
                f_args.update(prune_nones(f))
                self.loggit.debug('Filter args: {0}'.format(f_args))
                self.loggit.debug('Pre-instance: {0}'.format(self.indices))
                method(**f_args)
                self.loggit.debug('Post-instance: {0}'.format(self.indices))
            else:
                # Otherwise, it's a settingless filter.
                method()
