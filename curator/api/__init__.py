from .exceptions import *
from .utils import *
from .filter import *
from .alias import *
from .allocation import *
from .bloom import *
from .close import *
from .delete import *
from .opener import *
from .optimize import *
from .replicas import *
from .seal import *
from .show import *
from .snapshot import *

from datetime import timedelta, datetime, date
import time
import re
import logging

REGEX_MAP = {
    'timestring': r'^.*{0}.*$',
    'regex': r'{0}',
    'prefix': r'^{0}.*$',
    'suffix': r'^.*{0}$',
}

DATE_REGEX = {
    'Y' : '4',
    'y' : '2',
    'm' : '2',
    'W' : '2',
    'U' : '2',
    'd' : '2',
    'H' : '2',
    'M' : '2',
    'S' : '2',
    'j' : '3',
}

class index_list(object):
    def __init__(self, client):
        # Ignore mock type for testing
        if str(type(client)) == "<class 'mock.Mock'>":
            pass
        elif not type(client) == type(elasticsearch.Elasticsearch()):
            raise TypeError('client is of incorrect type: {0}'.format(type(client)))
        self.loggit = logging.getLogger('curator.api.index_list')
        self.client = client
        self.all_indices = []
        self.indices = []
        self.index_info = {}
        self.__get_indices()
        self.get_metadata()
        self.get_index_stats()

    def __get_indices(self):
        """
        Pull all indices into `self.all_indices`
        """
        try:
            self.all_indices = list(self.client.indices.get_settings(
                index='_all', params={'expand_wildcards': 'open,closed'}))
            self.loggit.debug('All indices: {0}'.format(self.all_indices))
        except Exception as e:
            raise RuntimeError('Failed to get indices.  Exception: {0}'.format(e))
        self.indices = self.all_indices
        self.empty_list_check()
        for index in self.indices:
            self.__build_index_info(index)

    def __build_index_info(self, index):
        """
        Ensure that `index` is a key in `self.index_info`. If not, create a
        sub-dictionary structure under that key.
        """
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

    def __chunkify(self, indices):
        """
        This utility segments very large index lists into smaller lists, where
        the size of the csv converted list would be no more than of 3KB.

        :arg indices: A list of indices to act on.
        :rtype: List of lists of indices.
        """
        chunks = []
        chunk = ""
        for index in indices:
            if len(chunk) < 3072:
                if not chunk:
                    chunk = index
                else:
                    chunk += "," + index
            else:
                chunks.append(chunk.split(','))
                chunk = index
        chunks.append(chunk.split(','))
        return chunks

    def empty_list_check(self):
        """Raise exception if `self.indices` is empty"""
        if not self.indices:
            raise NoIndices('index_list object is empty.')

    def __working_list(self):
        # Copy by value, rather than reference to prevent list stomping during iterations
        return self.indices[:]

    def get_index_stats(self):
        """
        Populate `self.index_info` with index `size_in_bytes` and doc count
        information for each index.
        """
        self.empty_list_check()
        # Subroutine to do the dirty work
        def iterate_over_stats(stats):
            for index in stats['indices']:
                size = stats['indices'][index]['total']['store']['size_in_bytes']
                docs = stats['indices'][index]['total']['docs']['count']
                self.loggit.info(
                    'Index: {0}  Size: {1}  Docs: {2}'.format(
                        index, byte_size(size), docs
                    )
                )
                self.index_info[index]['size_in_bytes'] = size
                self.index_info[index]['docs'] = docs

        working_list = self.__working_list()
        for index in self.__working_list():
            if self.index_info[index]['state'] == 'close':
                working_list.remove(index)
        if working_list:
            index_lists = self.__chunkify(working_list)
            for l in index_lists:
                iterate_over_stats(self.client.indices.stats(index=to_csv(l), metric='store,docs'))

    def get_metadata(self):
        """
        Populate `self.index_info` with index `size_in_bytes` and doc count
        information for each index.
        """
        self.empty_list_check()
        index_lists = self.__chunkify(self.indices)
        for l in index_lists:
            working_list = self.client.cluster.state(index=to_csv(l),metric='metadata')['metadata']['indices']
            if working_list:
                for index in list(working_list.keys()):
                    self.index_info[index]['age']['creation_date'] = working_list[index]['settings']['index']['creation_date']
                    self.index_info[index]['number_of_replicas'] = working_list[index]['settings']['index']['number_of_replicas']
                    self.index_info[index]['number_of_shards'] = working_list[index]['settings']['index']['number_of_shards']
                    self.index_info[index]['state'] = working_list[index]['state']
                    if 'routing' in working_list[index]['settings']['index']:
                        self.index_info[index]['routing'] = working_list[index]['settings']['index']['routing']

    def get_segmentcounts(self):
        """
        Populate `self.index_info` with segment information for each index.
        """
        self.empty_list_check()
        index_lists = self.__chunkify(self.indices)
        for l in index_lists:
            working_list = self.client.indices.segments(index=to_csv(l))['indices']
            if working_list:
                for index in list(working_list.keys()):
                    shards = working_list[index]['shards']
                    segmentcount = 0
                    for shardnum in shards:
                        for shard in range(0,len(shards[shardnum])):
                            segmentcount += shards[shardnum][shard]['num_search_segments']
                    self.index_info[index]['segments'] = segmentcount

    def get_name_based_ages(self, timestring):
        """
        Add indices to self.index_info based on the age as indicated by the index
        name pattern, if it matches `timestring`

        :arg timestring: An strftime pattern
        """
        # Check for empty list before proceeding here to prevent non-iterable condition
        self.empty_list_check()
        date_regex = get_date_regex(timestring)
        regex = r'(?P<date>{0})'.format(date_regex)
        pattern = re.compile(regex)
        for index in self.__working_list():
            match = pattern.search(index)
            if match:
                if match.group("date"):
                    timestamp = match.group("date")
                    epoch = (get_datetime(timestamp, timestring) - datetime(1970,1,1)).total_seconds()
                    self.index_info[index]['age']['name'] = int(epoch*1000)

    def get_stats_api_dates(self, field='@timestamp'):
        """
        Add indices to self.index_info based on the value the stats api returns,
        as determined by `field`

        :arg field: The field with the date value.  The field must be mapped in
            elasticsearch as a date datatype.  Default: ``@timestamp``
        """
        self.loggit.info('Cannot use stats_api on closed indices.  Pruning any closed indices.')
        self.prune_closed()
        index_lists = self.__chunkify(self.indices)
        for l in index_lists:
            working_list = self.client.field_stats(index=to_csv(l), fields=field, level='indices')['indices']
            if working_list:
                for index in list(working_list.keys()):
                    self.index_info[index]['age']['min_value'] = working_list[index]['fields'][field]['min_value']
                    self.index_info[index]['age']['max_value'] = working_list[index]['fields'][field]['max_value']

    def filter_by_regex(self, kind=None, value=None, exclude=False):
        """
        Prune indices not matching the pattern, or in the case of exclude,
        prune those matching the pattern.

        :arg kind: Can be one of: [suffix|prefix|regex|timestring].
            This option defines what kind of filter you will be building.
        :arg value: Depends on `kind`. It is the strftime string if `kind` is
            `timestring`. It's used to build the regular expression for other kinds.
        :arg exclude: Will cause the filter to invert, and keep non-matching values
        """
        if kind not in [ 'regex', 'prefix', 'suffix', 'timestring' ]:
            raise ValueError('{0}: Invalid value for kind'.format(kind))

        # Stop here if None or empty value, but zero is okay
        if value == 0:
            pass
        elif not value:
            raise ValueError('{0}: Invalid value for "value". Cannot be "None" type, empty, or False')

        if kind == 'timestring':
            regex = REGEX_MAP[kind].format(get_date_regex(value))
        else:
            regex = REGEX_MAP[kind].format(value)

        self.empty_list_check()
        pattern = re.compile(regex)
        for index in self.__working_list():
            match = pattern.match(index)
            if match:
                if exclude:
                    self.indices.remove(index)
            else:
                self.indices.remove(index)

    def filter_by_age(self, source='name', age=None, timestring=None,
        unit=None, unit_count=None, stat_value=None, field=None, epoch=None,
        ):
        """
        Remove indices from `self.indices` by relative age calculations.

        :arg source: Source of index age. Can be one of 'name', 'creation_date',
            or 'stats_api'
        :arg age: Age category to filter, either ``older`` or ``younger``
        """
        # Get timestamp point of reference, PoR
        PoR = get_point_of_reference(unit, unit_count, epoch)
        if source == 'name':
            self.get_name_based_ages(timestring)
        elif source == 'creation_date':
            # Nothing to do here as this comes from `get_metadata` in __init__
            pass
        elif source == 'stats_api':
            self.get_stats_api_dates(field=field)
        else:
            raise ValueError('Invalid source: {0}.  Must be one of "name", "creation_date", "stats_api".'.format(source))

        for index in list(self.index_info.keys()):
            if direction == 'older':
                # Remember, because time adds to epoch, smaller numbers are older
                # We want to remove values larger, or "younger," from the list
                # so downstream processing can be done on the "older" indices
                if self.index_info[index]['age'][source] > PoR:
                    self.indices.remove(index)
            elif direction == 'younger':
                # Remember, because time adds to epoch, larger numbers are younger
                # We want to remove values smaller, or "older," from the list
                # so downstream processing can be done on the "younger" indices
                if self.index_info[index]['age'][source] < PoR:
                    self.indices.remove(index)

    def filter_by_space(self, disk_space=None, reverse=True, use_age=False, age_type='creation_date'):
        """
        Remove indices from the provided list of indices based on space consumed,
        sorted reverse-alphabetically by default.  If you set `reverse` to
        `False`, it will be sorted alphabetically.

        The default is usually what you will want. If only one kind of index is
        provided--for example, indices matching ``logstash-%Y.%m.%d``--then reverse
        alphabetical sorting will mean the oldest get removed first, because lower
        numbers in the dates mean older indices.

        By setting reverse to `False`, then ``index3`` will be deleted before
        ``index2``, which will be deleted before ``index1``

        `use_age` allows ordering indices by age. Age is determined by the index
        creation date by default, but you can specify an `age_type` of ``name``,
        ``max_value``, or ``min_value``.  The ``name`` `age_type` require that the
        `get_name_based_ages` method be called with a timestring argument
        before it can be used.  The `get_stats_api_dates` must be run with either
        the default field value of ``@timestamp``, or with a different date field
        argument before the ``max_value`` and ``min_value`` `age_type`s can be used.

        :arg disk_space: Filter indices over *n* gigabytes
        :arg reverse: The filtering direction. (default: `True`).  Ignored if
            `use_age` is `True`
        :arg use_age: Sort indices by age.  `age_type` is required in this case.
        :arg age_type: One of ``name``, ``creation_date``, ``max_value``, or
            ``min_value``. Default: ``creation_date``
        """

        # Ensure that disk_space is a float
        if disk_space:
            disk_space = float(disk_space)
        else:
            raise ValueError("Missing value for disk_space.")

        disk_usage = 0.0
        disk_limit = disk_space * 2**30

        self.loggit.info('Cannot get disk usage info from closed indices.  Pruning any closed indices.')
        self.prune_closed()

        # Create a copy-by-value working list
        working_list = self.__working_list()

        if use_age:
            # Do the age-based sorting here.
            # First, build an intermediate dictionary with just index and age
            # as the key and value, respectively
            intermediate = {}
            for index in working_list:
                if not age_type in self.index_info[index]['age'].keys():
                    if age_type == 'name':
                        raise ValueError('Must run "get_name_based_ages" method first')
                    elif age_type == 'max_value' or age_type == 'min_value':
                        raise ValueError('Must run "get_stats_api_dates" method first')
                intermediate[index] = self.index_info[index]['age'][age_type]

            # This will sort the indices the youngest first. Effectively, this
            # should set us up to delete everything older than fits into
            # `disk_space`.  It starts as a tuple, but then becomes a list.
            sorted_tuple = sorted(intermediate.items(), key=lambda k: k[1], reverse=True)
            sorted_indices = [x[0] for x in sorted_tuple]

        else:
            # Default to sorting by index name
            sorted_indices = sorted(working_list, reverse=reverse)

        for index in sorted_indices:

            disk_usage += self.index_info[index]['size_in_bytes']
            suffix = '{0}, summed disk usage is {1} and disk limit is {2}.'.format(index, byte_size(disk_usage), byte_size(disk_limit))
            if disk_usage > disk_limit:
                verb = "Pruning"
                self.indices.remove(index)
            else:
                verb = "Keeping"

            self.loggit.info('{0}: {1}'.format(verb, suffix))

    def prune_kibana(self):
        """
        Prune any index named `.kibana`, `kibana-int`, `.marvel-kibana`, or
        `.marvel-es-data` from `self.indices`
        """
        self.empty_list_check()
        for index in ['.kibana', '.marvel-kibana', 'kibana-int', '.marvel-es-data']:
            if index in self.__working_list():
                self.indices.remove(index)

    def prune_forceMerged(self, max_num_segments=None):
        """
        Prune any index which has `max_num_segments` per shard or fewer from
        `indices`

        :arg max_num_segments: Cutoff number of segments per shard.
        """
        if not max_num_segments:
            raise ValueError('Missing value for "max_num_segments"')
        self.loggit.info('Cannot get segment count of closed indices.  Pruning any closed indices.')
        self.prune_closed()
        self.get_segmentcounts()
        for index in self.__working_list():
            # Do this to reduce long lines and make it more readable...
            shards = int(self.index_info[index]['number_of_shards'])
            replicas = int(self.index_info[index]['number_of_replicas'])
            segments = int(self.index_info[index]['segments'])
            suffix = '{0} has {1} shard(s) + {2} replica(s) with a sum total of {3} segments.'.format(
                index, shards, replicas, segments
            )
            expected_count = ((shards + (shards * replicas)) * max_num_segments)
            if segments <= expected_count:
                self.loggit.info('Pruning: {0} '.format(suffix))
                self.indices.remove(index)
            else:
                self.loggit.debug('Keeping: {0} '.format(suffix))

    def prune_closed(self):
        """
        Prune closed indices from `indices`
        """
        self.empty_list_check()
        for index in self.__working_list():
            if self.index_info[index]['state'] == 'close':
                self.loggit.info('Pruning closed index {0}'.format(index))
                self.indices.remove(index)

    def prune_opened(self):
        """
        Prune opened indices from `indices`
        """
        self.empty_list_check()
        for index in self.__working_list():
            if self.index_info[index]['state'] == 'open':
                self.loggit.info('Pruning opened index {0}'.format(index))
                self.indices.remove(index)

    def prune_allocated(self, key, value, allocation_type):
        """
        Prune all indices that have the routing allocation rule of `key=value`
        from `indices`

        :arg key: The allocation attribute to check for
        :arg value: The value to check for
        :arg allocation_type: Type of allocation to apply
        """
        self.empty_list_check()
        index_lists = self.__chunkify(self.indices)
        for l in index_lists:
            working_list = self.client.indices.get_settings(index=to_csv(l))
            if working_list:
                for index in list(working_list.keys()):
                    try:
                        has_routing = working_list[index]['settings']['index']['routing']['allocation'][allocation_type][key] == value
                    except KeyError:
                        has_routing = False
                    if has_routing:
                        self.loggit.debug(
                            'Pruning index {0}: index.routing.allocation.{1}.{2}={3} is already set.'.format(
                                index, allocation_type, key, value
                            )
                        )
                        self.indices.remove(index)
