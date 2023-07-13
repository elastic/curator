"""Index List Class"""
import re
import itertools
import logging
from elasticsearch8.exceptions import NotFoundError, TransportError
from es_client.helpers.utils import ensure_list
from curator.defaults import settings
from curator.exceptions import ActionError, ConfigurationError, MissingArgument, NoIndices
from curator.helpers.date_ops import (
    absolute_date_range, date_range, fix_epoch, get_date_regex, get_point_of_reference,
    get_unit_count_from_name, TimestringSearch
)
from curator.helpers.getters import byte_size, get_indices
from curator.helpers.testers import verify_client_object
from curator.helpers.utils import chunk_index_list, report_failure, to_csv
from curator.validators import SchemaCheck
from curator.validators.filter_functions import filterstructure

class IndexList:
    """IndexList class"""
    def __init__(self, client):
        verify_client_object(client)
        self.loggit = logging.getLogger('curator.indexlist')
        #: An :py:class:`~.elasticsearch.Elasticsearch` client object passed from param ``client``
        self.client = client
        #: Information extracted from indices, such as segment count, age, etc.
        #: Populated at instance creation time by private helper methods.
        #: **Type:** :py:class:`dict`
        self.index_info = {}
        #: The running list of indices which will be used by one of the :py:mod:`~.curator.actions`
        #: classes. Populated at instance creation time by private helper methods. **Type:**
        #: :py:class:`list`
        self.indices = []
        #: All indices in the cluster at instance creation time.
        #: **Type:** :py:class:`list`
        self.all_indices = []
        self.__get_indices()
        self.age_keyfield = None

    def __actionable(self, idx):
        self.loggit.debug('Index %s is actionable and remains in the list.', idx)

    def __not_actionable(self, idx):
        self.loggit.debug('Index %s is not actionable, removing from list.', idx)
        self.indices.remove(idx)

    def __excludify(self, condition, exclude, index, msg=None):
        if condition is True:
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
            self.loggit.debug('%s: %s', text, msg)

    def __get_indices(self):
        """
        Pull all indices into ``all_indices``, then populate ``indices`` and ``index_info``
        """
        self.loggit.debug('Getting all indices')
        self.all_indices = get_indices(self.client)
        self.indices = self.all_indices[:]
        if self.indices:
            for index in self.indices:
                self.__build_index_info(index)
            self.get_metadata()
            self.get_index_stats()

    def __build_index_info(self, index):
        """
        Ensure that ``index`` is a key in ``index_info``. If not, create a sub-dictionary structure
        under that key.
        """
        self.loggit.debug('Building preliminary index metadata for %s', index)
        if index not in self.index_info:
            self.index_info[index] = {
                "age" : {},
                "number_of_replicas" : 0,
                "number_of_shards" : 0,
                "segments" : 0,
                "size_in_bytes" : 0,
                "docs" : 0,
                "state" : "",
            }

    def __map_method(self, ftype):
        methods = {
            'alias': self.filter_by_alias,
            'age': self.filter_by_age,
            'allocated': self.filter_allocated,
            'closed': self.filter_closed,
            'count': self.filter_by_count,
            'empty': self.filter_empty,
            'forcemerged': self.filter_forceMerged,
            'ilm': self.filter_ilm,
            'kibana': self.filter_kibana,
            'none': self.filter_none,
            'opened': self.filter_opened,
            'period': self.filter_period,
            'pattern': self.filter_by_regex,
            'space': self.filter_by_space,
            'shards': self.filter_by_shards,
            'size': self.filter_by_size,
        }
        return methods[ftype]

    def __remove_missing(self, err):
        """
        Remove missing index found in ``err`` from self.indices and return that name
        """
        missing = err.info['error']['index']
        self.loggit.warning('Index was initiallly present, but now is not: %s', missing)
        self.loggit.debug('Removing %s from active IndexList')
        self.indices.remove(missing)
        return missing

    def get_index_stats(self):
        """
        Populate ``index_info`` with index ``size_in_bytes``, ``primary_size_in_bytes`` and doc
        count information for each index.
        """
        self.loggit.debug('Getting index stats')
        self.empty_list_check()
        # Subroutine to do the dirty work
        def iterate_over_stats(stats):
            for index in stats['indices']:
                size = stats['indices'][index]['total']['store']['size_in_bytes']
                docs = stats['indices'][index]['total']['docs']['count']
                primary_size = stats['indices'][index]['primaries']['store']['size_in_bytes']
                msg = (
                    f'Index: {index}  Size: {byte_size(size)}  Docs: {docs} '
                    f'PrimarySize: {byte_size(primary_size)}'
                )
                self.loggit.debug(msg)
                self.index_info[index]['size_in_bytes'] = size
                self.index_info[index]['docs'] = docs
                self.index_info[index]['primary_size_in_bytes'] = primary_size

        working_list = self.working_list()
        for index in self.working_list():
            if self.index_info[index]['state'] == 'close':
                working_list.remove(index)
        if working_list:
            index_lists = chunk_index_list(working_list)
            for lst in index_lists:
                stats_result = {}
                checking = True
                while checking:
                    try:
                        stats_result.update(self._get_indices_stats(lst))
                    except NotFoundError as err:
                        lst.remove(self.__remove_missing(err))
                        continue
                    except TransportError as err:
                        if '413' in err.errors:
                            msg = (
                                'Huge Payload 413 Err - '
                                'Trying to get information via multiple requests'
                            )
                            self.loggit.debug(msg)
                            stats_result = {}
                            try:
                                stats_result.update(self._bulk_queries(lst, self._get_indices_stats))
                            except NotFoundError as err2:
                                lst.remove(self.__remove_missing(err2))
                                continue
                    iterate_over_stats(stats_result)
                    checking = False

    def _get_indices_stats(self, data):
        return self.client.indices.stats(index=to_csv(data), metric='store,docs')

    def _bulk_queries(self, data, exec_func):
        slice_number = 10
        query_result = {}
        loop_number = round(len(data)/slice_number) if round(len(data)/slice_number) > 0 else 1
        self.loggit.debug("Bulk Queries - number requests created: %s", loop_number)
        for num in range(0, loop_number):
            if num == (loop_number-1):
                data_sliced = data[num*slice_number:]
            else:
                data_sliced = data[num*slice_number:(num+1)*slice_number]
            query_result.update(exec_func(data_sliced))
        return query_result

    def _get_cluster_state(self, data):
        return self.client.cluster.state(index=to_csv(data), metric='metadata')['metadata']['indices']

    def get_metadata(self):
        """
        Populate ``index_info`` with index ``size_in_bytes`` and doc count information for each
        index.
        """
        self.loggit.debug('Getting index metadata')
        self.empty_list_check()
        for lst in chunk_index_list(self.indices):
            working_list = {}
            # The API called by _get_cluster_state doesn't suffer from the same problems that
            # _get_indices_stats does. This won't result in an error if an index is suddenly
            # missing.
            try:
                working_list.update(self._get_cluster_state(lst))
            except TransportError as err:
                if '413' in err.errors:
                    msg = 'Huge Payload 413 Err - Trying to get information via multiple requests'
                    self.loggit.debug(msg)
                    working_list = {}
                    working_list.update(self._bulk_queries(lst, self._get_cluster_state))
            if working_list:
                for index in list(working_list.keys()):
                    sii = self.index_info[index]
                    wli = working_list[index]
                    sii['age']['creation_date'] = (
                        fix_epoch(wli['settings']['index']['creation_date'])
                    )
                    sii['number_of_replicas'] = wli['settings']['index']['number_of_replicas']
                    sii['number_of_shards'] = wli['settings']['index']['number_of_shards']
                    sii['state'] = wli['state']
                    if 'routing' in wli['settings']['index']:
                        sii['routing'] = wli['settings']['index']['routing']

    def empty_list_check(self):
        """Raise :py:exc:`~.curator.exceptions.NoIndices` if ``indices`` is empty"""
        self.loggit.debug('Checking for empty list')
        if not self.indices:
            raise NoIndices('index_list object is empty.')

    def working_list(self):
        """
        Return the current value of ``indices`` as copy-by-value to prevent list
        stomping during iterations
        """
        # Copy by value, rather than reference to prevent list stomping during
        # iterations
        self.loggit.debug('Generating working list of indices')
        return self.indices[:]

    def _get_indices_segments(self, data):
        return self.client.indices.segments(index=to_csv(data))['indices'].copy()

    def _get_segment_counts(self):
        """
        Populate ``index_info`` with segment information for each index.
        """
        self.loggit.debug('Getting index segment counts')
        self.empty_list_check()
        for lst in chunk_index_list(self.indices):
            working_list = {}
            try:
                working_list.update(self._get_indices_segments(lst))
            except TransportError as err:
                if '413' in err.errors:
                    self.loggit.debug('Huge Payload 413 Error - Trying to get information with multiple requests')
                    working_list = {}
                    working_list.update(self._bulk_queries(lst, self._get_indices_segments))
            if working_list:
                for index in list(working_list.keys()):
                    shards = working_list[index]['shards']
                    segmentcount = 0
                    for shardnum in shards:
                        for shard in range(0,len(shards[shardnum])):
                            segmentcount += (shards[shardnum][shard]['num_search_segments'])
                    self.index_info[index]['segments'] = segmentcount

    def _get_name_based_ages(self, timestring):
        """
        Add indices to ``index_info`` based on the age as indicated by the index
        name pattern, if it matches ``timestring``

        :param timestring: An :py:func:`time.strftime` pattern
        """
        # Check for empty list before proceeding here to prevent non-iterable condition
        self.loggit.debug('Getting ages of indices by "name"')
        self.empty_list_check()
        tstr = TimestringSearch(timestring)
        for index in self.working_list():
            epoch = tstr.get_epoch(index)
            if isinstance(epoch, int):
                self.index_info[index]['age']['name'] = epoch

    def _get_field_stats_dates(self, field='@timestamp'):
        """
        Add indices to ``index_info`` based on the values the queries return, as determined by the
        min and max aggregated values of ``field``

        :param field: The field with the date value.  The field must be mapped in elasticsearch as
            a date datatype. Default: ``@timestamp``
        """
        self.loggit.debug('Cannot query closed indices. Omitting any closed indices.')
        self.filter_closed()
        self.loggit.debug('Cannot use field_stats with empty indices. Omitting any empty indices.')
        self.filter_empty()
        self.loggit.debug(
            'Getting index date by querying indices for min & max value of %s field', field)
        self.empty_list_check()
        index_lists = chunk_index_list(self.indices)
        for lst in index_lists:
            for index in lst:
                aggs = {
                    'min' : { 'min' : { 'field' : field } },
                    'max' : { 'max' : { 'field' : field } }
                }
                response = self.client.search(index=index, size=0, aggs=aggs)
                self.loggit.debug('RESPONSE: %s', response)
                if response:
                    try:
                        res = response['aggregations']
                        self.loggit.debug('res: %s', res)
                        data = self.index_info[index]['age']
                        data['min_value'] = fix_epoch(res['min']['value'])
                        data['max_value'] = fix_epoch(res['max']['value'])
                        self.loggit.debug('data: %s', data)
                    except KeyError as exc:
                        raise ActionError(f'Field "{field}" not found in index "{index}"') from exc

    def _calculate_ages(self, source=None, timestring=None, field=None, stats_result=None):
        """
        This method initiates index age calculation based on the given parameters.  Exceptions are
        raised when they are improperly configured.

        Set instance variable ``age_keyfield`` for use later, if needed.

        :param source: Source of index age. Can be: ``name``, ``creation_date``, or ``field_stats``
        :param timestring: An :py:func:`time.strftime` string to match the datestamp in an index
            name. Only used for index filtering by ``name``.
        :param field: A timestamp field name.  Only used for ``field_stats`` based calculations.
        :param stats_result: Either ``min_value`` or ``max_value``.  Only used in conjunction with
            ``source=field_stats`` to choose whether to reference the min or max result value.
        """
        self.age_keyfield = source
        if source == 'name':
            if not timestring:
                raise MissingArgument('source "name" requires the "timestring" keyword argument')
            self._get_name_based_ages(timestring)
        elif source == 'creation_date':
            # Nothing to do here as this comes from `get_metadata` in __init__
            pass
        elif source == 'field_stats':
            if not field:
                raise MissingArgument('source "field_stats" requires the "field" keyword argument')
            if stats_result not in ['min_value', 'max_value']:
                raise ValueError(f'Invalid value for "stats_result": {stats_result}')
            self.age_keyfield = stats_result
            self._get_field_stats_dates(field=field)
        else:
            raise ValueError(
                f'Invalid source: {source}. Must be one of "name", "creation_date", "field_stats".'
            )

    def _sort_by_age(self, index_list, reverse=True):
        """
        Take a list of indices and sort them by date.

        By default, the youngest are first with ``reverse=True``, but the oldest can be first by
        setting ``reverse=False``
        """
        # Do the age-based sorting here.
        # Build an temporary dictionary with just index and age as the key and value, respectively
        temp = {}
        for index in index_list:
            if self.age_keyfield in self.index_info[index]['age']:
                temp[index] = self.index_info[index]['age'][self.age_keyfield]
            else:
                msg = (f'{index} does not have key "{self.age_keyfield}" in IndexList metadata')
                self.__excludify(True, True, index, msg)
        # Sort alphabetically prior to age sort to keep sorting consistent
        temp_tuple = (sorted(temp.items(), key=lambda k: k[0], reverse=reverse))
        # If reverse is True, this will sort so the youngest indices are first.
        # However, if you want oldest first, set reverse to False.
        # Effectively, this should set us up to act on everything older than
        # meets the other set criteria. It starts as a tuple, but then becomes a list.
        sorted_tuple = (sorted(temp_tuple, key=lambda k: k[1], reverse=reverse))
        return [x[0] for x in sorted_tuple]

    def filter_by_regex(self, kind=None, value=None, exclude=False):
        """
        Match indices by regular expression (pattern).

        :param kind: Can be one of: ``suffix``, ``prefix``, ``regex``, or ``timestring``. This
            option defines what ``kind`` of filter you will be building.
        :param value: Depends on ``kind``. It is the :py:func:`time.strftime` string if ``kind``
            is ``timestring``. It's used to build the regular expression for other kinds.
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``False``
        """
        self.loggit.debug('Filtering indices by regex')
        if kind not in [ 'regex', 'prefix', 'suffix', 'timestring' ]:
            raise ValueError(f'{kind}: Invalid value for kind')
        # Stop here if None or empty value, but zero is okay
        if value == 0:
            pass
        elif not value:
            raise ValueError(
                'Invalid None value for "value". Cannot be "None" type, empty, or False')
        if kind == 'timestring':
            regex = settings.regex_map()[kind].format(get_date_regex(value))
        else:
            regex = settings.regex_map()[kind].format(value)
        self.empty_list_check()
        pattern = re.compile(regex)
        for index in self.working_list():
            self.loggit.debug('Filter by regex: Index: %s', index)
            match = pattern.search(index)
            if match:
                self.__excludify(True, exclude, index)
            else:
                self.__excludify(False, exclude, index)

    def filter_by_age(self, source='name', direction=None, timestring=None, unit=None,
        unit_count=None, field=None, stats_result='min_value', epoch=None, exclude=False,
        unit_count_pattern=False):
        """
        Match indices by relative age calculations.

        :param source: Source of index age. Can be one of ``name``, ``creation_date``, or
            ``field_stats``
        :param direction: Time to filter, either ``older`` or ``younger``
        :param timestring: An :py:func:`time.strftime` string to match the datestamp in an index
            name. Only used for index filtering by ``name``.
        :param unit: One of ``seconds``, ``minutes``, ``hours``, ``days``, ``weeks``, ``months``,
            or ``years``.
        :param unit_count: The count of ``unit``. ``unit_count`` * ``unit`` will be calculated out
            to the relative number of seconds.
        :param unit_count_pattern: A regular expression whose capture group identifies the value
            for ``unit_count``.
        :param field: A timestamp field name.  Only used for ``field_stats`` based calculations.
        :param stats_result: Either ``min_value`` or ``max_value``.  Only used in conjunction with
            ``source=field_stats`` to choose whether to reference the minimum or maximum result
            value.
        :param epoch: An epoch timestamp used in conjunction with ``unit`` and ``unit_count`` to
            establish a point of reference for calculations. If not provided, the current time will
            be used.
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude`` is `False`, then only matching indices will be kept in
            ``indices``. Default is ``False``
        """
        self.loggit.debug('Filtering indices by age')
        # Get timestamp point of reference, PoR
        PoR = get_point_of_reference(unit, unit_count, epoch)
        if not direction:
            raise MissingArgument('Must provide a value for "direction"')
        if direction not in ['older', 'younger']:
            raise ValueError(f'Invalid value for "direction": {direction}')
        self._calculate_ages(
            source=source, timestring=timestring, field=field, stats_result=stats_result)
        if unit_count_pattern:
            try:
                unit_count_matcher = re.compile(unit_count_pattern)
            # pylint: disable=broad-except
            except Exception as exc:
                # We got an illegal regex, so won't be able to match anything
                self.loggit.error(
                    'Regular expression failure. Will not match unit count. Error: %s', exc)
                unit_count_matcher = None
        for index in self.working_list():
            try:
                removeThisIndex = False
                age = int(self.index_info[index]['age'][self.age_keyfield])
                msg = (
                    f'Index "{index}" age ({age}), direction: "{direction}", point of '
                    f'reference, ({PoR})'
                )
                # Because time adds to epoch, smaller numbers are actually older
                # timestamps.
                if unit_count_pattern:
                    self.loggit.debug('Unit_count_pattern is set, trying to match pattern to index "%s"', index)
                    unit_count_from_index = get_unit_count_from_name(index, unit_count_matcher)
                    if unit_count_from_index:
                        self.loggit.debug('Pattern matched, applying unit_count of  "%s"', unit_count_from_index)
                        adjustedPoR = get_point_of_reference(unit, unit_count_from_index, epoch)
                        msg = (
                            f'Adjusting point of reference from {PoR} to {adjustedPoR} '
                            f'based on unit_count of {unit_count_from_index} from index name'
                        )
                        self.loggit.debug(msg)
                    elif unit_count == -1:
                        # Unable to match pattern and unit_count is -1, meaning no fallback, so this
                        # index is removed from the list
                        msg = (
                            f'Unable to match pattern and no fallback value set. '
                            f'Removing index "{index}" from actionable list'
                        )
                        self.loggit.debug(msg)
                        removeThisIndex = True
                        adjustedPoR = PoR # necessary to avoid exception if the first index is excluded
                    else:
                        # Unable to match the pattern and unit_count is set, so fall back to using unit_count
                        # for determining whether to keep this index in the list
                        self.loggit.debug('Unable to match pattern using fallback value of "%s"', unit_count)
                        adjustedPoR = PoR
                else:
                    adjustedPoR = PoR
                if direction == 'older':
                    agetest = age < adjustedPoR
                else:
                    agetest = age > adjustedPoR
                self.__excludify(agetest and not removeThisIndex, exclude, index, msg)
            except KeyError:
                msg = (
                    f'Index "{index}" does not meet provided criteria. '
                    f'Removing from list.'
                )
                self.loggit.debug(msg)
                self.indices.remove(index)

    def filter_by_space(self, disk_space=None, reverse=True, use_age=False, source='creation_date',
            timestring=None, field=None, stats_result='min_value', exclude=False,
            threshold_behavior='greater_than'):
        """
        Remove indices from the actionable list based on space consumed, sorted
        reverse-alphabetically by default.  If you set ``reverse`` to ``False``, it will be sorted
        alphabetically.

        The default is usually what you will want. If only one kind of index is provided--for
        example, indices matching ``logstash-%Y.%m.%d`` --then reverse alphabetical sorting will
        mean the oldest will remain in the list, because lower numbers in the dates mean older
        indices.

        By setting ``reverse`` to ``False``, then ``index3`` will be deleted before ``index2``,
        which will be deleted before ``index1``

        ``use_age`` allows ordering indices by age. Age is determined by the index creation date by
        default, but you can specify an ``source`` of ``name``, ``max_value``, or ``min_value``.
        The ``name`` ``source`` requires the timestring argument.

        ``threshold_behavior``, when set to ``greater_than`` (default), includes if it the index
        tests to be larger than ``disk_space``. When set to ``less_than``, it includes if the index
        is smaller than ``disk_space``

        :param disk_space: Filter indices over *n* gigabytes
        :param threshold_behavior: Size to filter, either ``greater_than`` or ``less_than``.
            Defaults to ``greater_than`` to preserve backwards compatability.
        :param reverse: The filtering direction. (default: ``True``).  Ignored if ``use_age`` is
            ``True``
        :param use_age: Sort indices by age.  ``source`` is required in this case.
        :param source: Source of index age. Can be one of ``name``, ``creation_date``, or
            ``field_stats``. Default: ``creation_date``
        :param timestring: An :py:func:`time.strftime` string to match the datestamp in an index
            name. Only used if ``source=name`` is selected.
        :param field: A timestamp field name.  Only used if ``source=field_stats`` is selected.
        :param stats_result: Either ``min_value`` or ``max_value``.  Only used if
            ``source=field_stats`` is selected. It determines whether to reference the minimum or
            maximum value of `field` in each index.
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``False``
        """
        self.loggit.debug('Filtering indices by disk space')
        # Ensure that disk_space is a float
        if not disk_space:
            raise MissingArgument('No value for "disk_space" provided')
        if threshold_behavior not in ['greater_than', 'less_than']:
            raise ValueError(f'Invalid value for "threshold_behavior": {threshold_behavior}')
        disk_space = float(disk_space)
        disk_usage = 0.0
        disk_limit = disk_space * 2**30
        self.loggit.debug(
            'Cannot get disk usage info from closed indices. Omitting any closed indices.')
        self.filter_closed()
        # Create a copy-by-value working list
        working_list = self.working_list()
        if use_age:
            self._calculate_ages(
                source=source, timestring=timestring, field=field, stats_result=stats_result)
            # Using default value of reverse=True in self._sort_by_age()
            sorted_indices = self._sort_by_age(working_list)
        else:
            # Default to sorting by index name
            sorted_indices = sorted(working_list, reverse=reverse)
        for index in sorted_indices:
            disk_usage += self.index_info[index]['size_in_bytes']
            msg = (
                f'{index}, summed disk usage is {byte_size(disk_usage)} and disk limit '
                f'is {byte_size(disk_limit)}.'
            )
            if threshold_behavior == 'greater_than':
                self.__excludify((disk_usage > disk_limit), exclude, index, msg)
            elif threshold_behavior == 'less_than':
                self.__excludify((disk_usage < disk_limit), exclude, index, msg)

    def filter_kibana(self, exclude=True):
        """
        Match any index named ``.kibana*`` in ``indices``. Older releases addressed index names
        that no longer exist.

        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``True``
        """
        self.loggit.debug('Filtering kibana indices')
        self.empty_list_check()
        for index in self.working_list():
            pattern = re.compile(r'^\.kibana.*$')
            if pattern.match(index):
                self.__excludify(True, exclude, index)
            else:
                self.__excludify(False, exclude, index)

    def filter_forceMerged(self, max_num_segments=None, exclude=True):
        """
        Match any index which has ``max_num_segments`` per shard or fewer in the actionable list.

        :param max_num_segments: Cutoff number of segments per shard.
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``True``
        """
        self.loggit.debug('Filtering forceMerged indices')
        if not max_num_segments:
            raise MissingArgument('Missing value for "max_num_segments"')
        self.loggit.debug(
            'Cannot get segment count of closed indices. Omitting any closed indices.')
        self.filter_closed()
        self._get_segment_counts()
        for index in self.working_list():
            # Do this to reduce long lines and make it more readable...
            shards = int(self.index_info[index]['number_of_shards'])
            replicas = int(self.index_info[index]['number_of_replicas'])
            segments = int(self.index_info[index]['segments'])
            msg = (
                f'{index} has {shards} shard(s) + {replicas} replica(s) '
                f'with a sum total of {segments} segments.'
            )
            expected_count = ((shards + (shards * replicas)) * max_num_segments)
            self.__excludify((segments <= expected_count), exclude, index, msg)

    def filter_closed(self, exclude=True):
        """
        Filter out closed indices from ``indices``

        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``True``
        """
        self.loggit.debug('Filtering closed indices')
        self.empty_list_check()
        for index in self.working_list():
            condition = self.index_info[index]['state'] == 'close'
            self.loggit.debug('Index %s state: %s', index, self.index_info[index]['state'])
            self.__excludify(condition, exclude, index)

    def filter_empty(self, exclude=True):
        """
        Filter indices with a document count of zero. Indices that are closed are automatically
        excluded from consideration due to closed indices reporting a document count of zero.

        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``True``
        """
        self.loggit.debug('Filtering empty indices')
        self.filter_closed()
        self.empty_list_check()
        for index in self.working_list():
            condition = self.index_info[index]['docs'] == 0
            self.loggit.debug('Index %s doc count: %s', index, self.index_info[index]['docs'])
            self.__excludify(condition, exclude, index)

    def filter_opened(self, exclude=True):
        """
        Filter out opened indices from ``indices``

        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``True``
        """
        self.loggit.debug('Filtering open indices')
        self.empty_list_check()
        for index in self.working_list():
            condition = self.index_info[index]['state'] == 'open'
            self.loggit.debug('Index %s state: %s', index, self.index_info[index]['state'])
            self.__excludify(condition, exclude, index)

    def filter_allocated(self, key=None, value=None, allocation_type='require', exclude=True):
        """
        Match indices that have the routing allocation rule of ``key=value`` from ``indices``

        :param key: The allocation attribute to check for
        :param value: The value to check for
        :param allocation_type: Type of allocation to apply
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is `T`rue`
        """
        self.loggit.debug('Filtering indices with shard routing allocation rules')
        if not key:
            raise MissingArgument('No value for "key" provided')
        if not value:
            raise MissingArgument('No value for "value" provided')
        if allocation_type not in ['include', 'exclude', 'require']:
            raise ValueError(f'Invalid "allocation_type": {allocation_type}')
        self.empty_list_check()
        for lst in chunk_index_list(self.indices):
            working_list = self.client.indices.get_settings(index=to_csv(lst))
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
                        f'{index}: Routing (mis)match: '
                        f'index.routing.allocation.{allocation_type}.{key}={value}.'
                    )
                    self.__excludify(has_routing, exclude, index, msg)

    def filter_none(self):
        """The legendary NULL filter"""
        self.loggit.debug('"None" filter selected.  No filtering will be done.')

    def filter_by_alias(self, aliases=None, exclude=False):
        """
        Match indices which are associated with the alias or list of aliases identified by
        ``aliases``. Indices must appear in all aliases in list ``aliases`` or a 404 error will
        result, leading to no indices being matched.

        :param aliases: A list of alias names.
        :type aliases: list
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``False``
        """
        self.loggit.debug('Filtering indices matching aliases: "%s"', aliases)
        if not aliases:
            raise MissingArgument('No value for "aliases" provided')
        aliases = ensure_list(aliases)
        self.empty_list_check()
        for lst in chunk_index_list(self.indices):
            try:
                # get_alias will either return {} or a NotFoundError.
                has_alias = list(self.client.indices.get_alias(
                                    index=to_csv(lst), name=to_csv(aliases)).keys())
                self.loggit.debug('has_alias: %s', has_alias)
            except NotFoundError:
                # if we see the NotFoundError, we need to set working_list to {}
                has_alias = []
            for index in lst:
                if index in has_alias:
                    isness = 'is'
                    condition = True
                else:
                    isness = 'is not'
                    condition = False
                msg = f'{index} {isness} associated with aliases: {aliases}'
                self.__excludify(condition, exclude, index, msg)

    def filter_by_count(self, count=None, reverse=True, use_age=False, pattern=None,
            source='creation_date', timestring=None, field=None, stats_result='min_value',
            exclude=True):
        # pylint: disable=anomalous-backslash-in-string
        """
        Remove indices from the actionable list beyond the number ``count``, sorted
        reverse-alphabetically by default.  If you set ``reverse=False``, it will be sorted
        alphabetically.

        The default is usually what you will want. If only one kind of index is provided--for
        example, indices matching ``logstash-%Y.%m.%d`` -- then reverse alphabetical sorting will
        mean the oldest will remain in the list, because lower numbers in the dates mean older
        indices.

        By setting ``reverse=False``, then ``index3`` will be deleted before ``index2``, which will
        be deleted before ``index1``

        ``use_age`` allows ordering indices by age. Age is determined by the index creation date by
        default, but you can specify an ``source`` of ``name``, ``max_value``, or ``min_value``.
        The ``name`` `source` requires the timestring argument.

        :param count: Filter indices beyond ``count``.
        :param reverse: The filtering direction. (default: ``True``).
        :param use_age: Sort indices by age.  ``source`` is required in this case.
        :param pattern: Select indices to count from a regular expression pattern. This pattern
            must have one and only one capture group. This can allow a single ``count`` filter
            instance to operate against any number of matching patterns, and keep ``count`` of each
            index in that group.  For example, given a ``pattern`` of ``'^(.*)-\d{6}$'``, it will
            match both ``rollover-000001`` and ``index-999990``, but not ``logstash-2017.10.12``.
            Following the same example, if my cluster also had ``rollover-000002`` through
            ``rollover-000010`` and ``index-888888`` through ``index-999999``, it will process both
            groups of indices, and include or exclude the ``count`` of each.
        :param source: Source of index age. Can be one of ``name``, ``creation_date``, or
            ``field_stats``. Default: ``creation_date``
        :param timestring: An :py:func:`time.strftime` string to match the datestamp in an index
            name. Only used if ``source=name``.
        :param field: A timestamp field name.  Only used if ``source=field_stats``.
        :param stats_result: Either ``min_value`` or ``max_value``.  Only used if
            ``source=field_stats``. It determines whether to reference the minimum or maximum value
            of ``field`` in each index.
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``True``
        """
        self.loggit.debug('Filtering indices by count')
        if not count:
            raise MissingArgument('No value for "count" provided')
        # Create a copy-by-value working list
        working_list = self.working_list()
        if pattern:
            try:
                regex = re.compile(pattern)
                if regex.groups < 1:
                    raise ConfigurationError(f'No regular expression group found in {pattern}')
                if regex.groups > 1:
                    raise ConfigurationError(
                        f'More than 1 regular expression group found in {pattern}')
                # Prune indices not matching the regular expression the object
                # (And filtered_indices) We do not want to act on them by accident.
                prune_these = list(filter(lambda x: regex.match(x) is None, working_list))
                filtered_indices = working_list
                for index in prune_these:
                    msg = '{index} does not match regular expression {pattern}.'
                    condition = True
                    exclude = True
                    self.__excludify(condition, exclude, index, msg)
                    # also remove it from filtered_indices
                    filtered_indices.remove(index)
                # Presort these filtered_indices using the lambda
                presorted = sorted(filtered_indices, key=lambda x: regex.match(x).group(1))
            except Exception as exc:
                raise ActionError(f'Unable to process pattern: "{pattern}". Error: {exc}') from exc
            # Initialize groups here
            groups = []
            # We have to pull keys k this way, but we don't need to keep them
            # We only need g for groups
            for _, g in itertools.groupby(presorted, key=lambda x: regex.match(x).group(1)):
                groups.append(list(g))
        else:
            # Since pattern will create a list of lists, and we iterate over that,
            # we need to put our single list inside a list
            groups = [ working_list ]
        for group in groups:
            if use_age:
                if source != 'name':
                    self.loggit.warning(
                        'Cannot get age information from closed indices unless '
                        'source="name".  Omitting any closed indices.'
                    )
                    self.filter_closed()
                self._calculate_ages(
                    source=source, timestring=timestring, field=field, stats_result=stats_result)
                # Using default value of reverse=True in self._sort_by_age()
                sorted_indices = self._sort_by_age(group, reverse=reverse)
            else:
                # Default to sorting by index name
                sorted_indices = sorted(group, reverse=reverse)
            idx = 1
            for index in sorted_indices:
                msg = f'{index} is {idx} of specified count of {count}.'
                condition = True if idx <= count else False
                self.__excludify(condition, exclude, index, msg)
                idx += 1

    def filter_by_shards(self, number_of_shards=None, shard_filter_behavior='greater_than', exclude=False):
        """
        Match ``indices`` with a given shard count.

        Selects all indices with a shard count ``greater_than`` ``number_of_shards`` by default.
        Use ``shard_filter_behavior`` to select indices with shard count ``greater_than``,
        ``greater_than_or_equal``, ``less_than``, ``less_than_or_equal``, or ``equal`` to
        ``number_of_shards``.

        :param number_of_shards: shard threshold
        :param shard_filter_behavior: Do you want to filter on ``greater_than``,
            ``greater_than_or_equal``, ``less_than``, ``less_than_or_equal``, or ``equal``?
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``False``
        """
        self.loggit.debug("Filtering indices by number of shards")
        if not number_of_shards:
            raise MissingArgument('No value for "number_of_shards" provided')
        if shard_filter_behavior not in ['greater_than', 'less_than', 'greater_than_or_equal', 'less_than_or_equal', 'equal']:
            raise ValueError(f'Invalid value for "shard_filter_behavior": {shard_filter_behavior}')
        if number_of_shards < 1 or (shard_filter_behavior == 'less_than' and number_of_shards == 1):
            raise ValueError(
                f'Unacceptable value: {number_of_shards} -- "number_of_shards" cannot be less '
                f'than 1. A valid index will have at least one shard.'
            )
        self.empty_list_check()
        for index in self.working_list():
            self.loggit.debug('Filter by number of shards: Index: %s', index)
            if shard_filter_behavior == 'greater_than':
                condition = int(self.index_info[index]['number_of_shards']) > number_of_shards
            elif shard_filter_behavior == 'less_than':
                condition = int(self.index_info[index]['number_of_shards']) < number_of_shards
            elif shard_filter_behavior == 'greater_than_or_equal':
                condition = int(self.index_info[index]['number_of_shards']) >= number_of_shards
            elif shard_filter_behavior == 'less_than_or_equal':
                condition = int(self.index_info[index]['number_of_shards']) <= number_of_shards
            else:
                condition = int(self.index_info[index]['number_of_shards']) == number_of_shards
            self.__excludify(condition, exclude, index)

    def filter_period(self, period_type='relative', source='name', range_from=None, range_to=None,
        date_from=None, date_to=None, date_from_format=None, date_to_format=None, timestring=None,
        unit=None, field=None, stats_result='min_value', intersect=False, week_starts_on='sunday',
        epoch=None, exclude=False):
        """
        Match ``indices`` with ages within a given period.

        :param period_type: Can be either ``absolute`` or ``relative``.  Default is ``relative``.
            ``date_from`` and ``date_to`` are required when using ``period_type='absolute'``.
            ``range_from`` and ``range_to`` are required with ``period_type='relative'``.
        :param source: Source of index age. Can be ``name``, ``creation_date``, or ``field_stats``
        :param range_from: How many ``unit`` (s) in the past/future is the origin?
        :param range_to: How many ``unit`` (s) in the past/future is the end point?
        :param date_from: The simplified date for the start of the range
        :param date_to: The simplified date for the end of the range.  If this value is the same as
            ``date_from``, the full value of ``unit`` will be extrapolated for the range.  For
            example, if ``unit=months``, and ``date_from`` and ``date_to`` are both
            ``2017.01``, then the entire month of January 2017 will be the absolute date range.
        :param date_from_format: The :py:func:`time.strftime` string used to parse ``date_from``
        :param date_to_format: The :py:func:`time.strftime` string used to parse ``date_to``
        :param timestring: An :py:func:`time.strftime` string to match the datestamp in an index
            name. Only used for index filtering by ``name``.
        :param unit: One of ``hours``, ``days``, ``weeks``, ``months``, or ``years``.
        :param field: A timestamp field name.  Only used for ``field_stats`` based calculations.
        :param stats_result: Either ``min_value`` or ``max_value``.  Only used in conjunction with
            ``source='field_stats'`` to choose whether to reference the min or max result value.
        :param intersect: Only used when ``source='field_stats'``. If ``True``, only indices where
            both ``min_value`` and ``max_value`` are within the period will be selected. If
            ``False``, it will use whichever you specified.  Default is ``False`` to preserve
            expected behavior.
        :param week_starts_on: Either ``sunday`` or ``monday``. Default is ``sunday``
        :param epoch: An epoch timestamp used to establish a point of reference for calculations.
            If not provided, the current time will be used.
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``False``
        """
        self.loggit.debug('Filtering indices by period')
        if period_type not in ['absolute', 'relative']:
            raise ValueError(
                f'Unacceptable value: {period_type} -- "period_type" must be either "absolute" or '
                f'"relative".'
            )
        if period_type == 'relative':
            func = date_range
            args = [unit, range_from, range_to, epoch]
            kwgs = { 'week_starts_on': week_starts_on }
            if (not isinstance(range_from, int)) or (not isinstance(range_to, int)):
                raise ConfigurationError('"range_from" and "range_to" must be integer values')
        else:
            func = absolute_date_range
            args = [unit, date_from, date_to]
            kwgs = { 'date_from_format': date_from_format, 'date_to_format': date_to_format }
            for reqd in [date_from, date_to, date_from_format, date_to_format]:
                if not reqd:
                    raise ConfigurationError(
                        'Must provide "date_from", "date_to", "date_from_format", and '
                        '"date_to_format" with absolute period_type'
                    )
        try:
            start, end = func(*args, **kwgs)
        # pylint: disable=broad-except
        except Exception as exc:
            report_failure(exc)
        self._calculate_ages(
            source=source, timestring=timestring, field=field, stats_result=stats_result)
        for index in self.working_list():
            try:
                if source == 'field_stats' and intersect:
                    min_age = int(self.index_info[index]['age']['min_value'])
                    max_age = int(self.index_info[index]['age']['max_value'])
                    msg = (
                        f'Index "{index}", timestamp field "{field}", min_value ({min_age}), '
                        f'max_value ({max_age}), period start: "{start}", period '
                        f'end, "{end}"'
                    )
                    # Because time adds to epoch, smaller numbers are actually older
                    # timestamps.
                    inrange = ((min_age >= start) and (max_age <= end))
                else:
                    age = int(self.index_info[index]['age'][self.age_keyfield])
                    msg = (
                        f'Index "{index}" age ({age}), period start: "{start}", period '
                        f'end, "{end}"'
                    )
                    # Because time adds to epoch, smaller numbers are actually older
                    # timestamps.
                    inrange = ((age >= start) and (age <= end))
                self.__excludify(inrange, exclude, index, msg)
            except KeyError:
                self.loggit.debug(
                    'Index "%s" does not meet provided criteria. Removing from list.', index)
                self.indices.remove(index)

    def filter_ilm(self, exclude=True):
        """
        Match indices that have the setting ``index.lifecycle.name``

        :param exclude: If ``exclude=True``, this filter will remove matching ``indices``. If
            ``exclude=False``, then only matching indices will be kept in ``indices``. Default is
            ``True``
        """
        self.loggit.debug('Filtering indices with index.lifecycle.name')
        index_lists = chunk_index_list(self.indices)
        if index_lists == [['']]:
            self.loggit.debug('Empty working list. No ILM indices to filter.')
            return
        for lst in index_lists:
            working_list = self.client.indices.get_settings(index=to_csv(lst))
            if working_list:
                for index in list(working_list.keys()):
                    try:
                        subvalue = working_list[index]['settings']['index']['lifecycle']
                        has_ilm = 'name' in subvalue
                        msg = f"{index} has index.lifecycle.name {subvalue['name']}"
                    except KeyError:
                        has_ilm = False
                        msg = f'index.lifecycle.name is not set for index {index}'
                    self.__excludify(has_ilm, exclude, index, msg)

    def iterate_filters(self, filter_dict):
        """
        Iterate over the filters defined in ``config`` and execute them.

        :param filter_dict: The configuration dictionary

        .. note:: ``filter_dict`` should be a dictionary with the following form:
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
            self.loggit.info('No filters in config.  Returning unaltered object.')
            return
        self.loggit.debug('All filters: %s', filter_dict['filters'])
        for fil in filter_dict['filters']:
            self.loggit.debug('Top of the loop: %s', self.indices)
            self.loggit.debug('Un-parsed filter args: %s', fil)
            # Make sure we got at least this much in the configuration
            chk = SchemaCheck(
                fil, filterstructure(), 'filter', 'IndexList.iterate_filters').result()
            msg = f'Parsed filter args: {chk}'
            self.loggit.debug(msg)
            method = self.__map_method(fil['filtertype'])
            del fil['filtertype']
            # If it's a filtertype with arguments, update the defaults with the
            # provided settings.
            if fil:
                self.loggit.debug('Filter args: %s', fil)
                self.loggit.debug('Pre-instance: %s', self.indices)
                method(**fil)
                self.loggit.debug('Post-instance: %s', self.indices)
            else:
                # Otherwise, it's a settingless filter.
                method()

    def filter_by_size(
        self, size_threshold=None, threshold_behavior='greater_than', exclude=False,
        size_behavior='primary'):
        """
        Remove indices from the actionable list based on index size.

        ``threshold_behavior``, when set to ``greater_than`` (default), includes if it the index
        tests to be larger than ``size_threshold``. When set to ``less_than``, it includes if
        the index is smaller than ``size_threshold``

        :param size_threshold: Filter indices over *n* gigabytes
        :param threshold_behavior: Size to filter, either ``greater_than`` or ``less_than``.
            Defaults to ``greater_than`` to preserve backwards compatability.
        :param size_behavior: Size that used to filter, either ``primary`` or ``total``. Defaults
            to ``primary``
        :param exclude: If ``exclude=True``, this filter will remove matching indices from
            ``indices``. If ``exclude=False``, then only matching indices will be kept in
            ``indices``. Default is ``False``
        """
        self.loggit.debug('Filtering indices by index size')
        # Ensure that disk_space is a float
        if not size_threshold:
            raise MissingArgument('No value for "size_threshold" provided')
        if size_behavior not in ['primary', 'total']:
            raise ValueError(f'Invalid value for "size_behavior": {size_behavior}')
        if threshold_behavior not in ['greater_than', 'less_than']:
            raise ValueError(f'Invalid value for "threshold_behavior": {threshold_behavior}')
        index_size_limit = float(size_threshold) * 2**30
        self.loggit.debug(
            'Cannot get disk usage info from closed indices. Omitting any closed indices.')
        self.filter_closed()
        # Create a copy-by-value working list
        working_list = self.working_list()
        for index in working_list:
            if size_behavior == 'primary':
                index_size = self.index_info[index]['primary_size_in_bytes']
            else:
                index_size = self.index_info[index]['size_in_bytes']
            msg = (
                f'{index}, index size is {byte_size(index_size)} and '
                f'size limit is {byte_size(index_size_limit)}.'
            )
            if threshold_behavior == 'greater_than':
                self.__excludify((index_size > index_size_limit), exclude, index, msg)
            elif threshold_behavior == 'less_than':
                self.__excludify((index_size < index_size_limit), exclude, index, msg)
