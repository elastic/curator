import time
import re
import logging
from datetime import timedelta, datetime, date
from curator import exceptions, utils
from curator.defaults import settings
from curator.validators import SchemaCheck, filters

class SnapshotList(object):
    def __init__(self, client, repository=None):
        utils.verify_client_object(client)
        if not repository:
            raise exceptions.MissingArgument('No value for "repository" provided')
        if not utils.repository_exists(client, repository):
            raise exceptions.FailedExecution(
                'Unable to verify existence of repository '
                '{0}'.format(repository)
            )
        self.loggit = logging.getLogger('curator.snapshotlist')
        #: An Elasticsearch Client object.
        #: Also accessible as an instance variable.
        self.client = client
        #: An Elasticsearch repository.
        #: Also accessible as an instance variable.
        self.repository = repository
        #: Instance variable.
        #: Information extracted from snapshots, such as age, etc.
        #: Populated by internal method `__get_snapshots` at instance creation
        #: time. **Type:** ``dict()``
        self.snapshot_info = {}
        #: Instance variable.
        #: The running list of snapshots which will be used by an Action class.
        #: Populated by internal methods `__get_snapshots` at instance creation
        #: time. **Type:** ``list()``
        self.snapshots = []
        #: Instance variable.
        #: Raw data dump of all snapshots in the repository at instance creation
        #: time.  **Type:** ``list()`` of ``dict()`` data.
        self.__get_snapshots()


    def __actionable(self, snap):
        self.loggit.debug(
            'Snapshot {0} is actionable and remains in the list.'.format(snap))

    def __not_actionable(self, snap):
            self.loggit.debug(
                'Snapshot {0} is not actionable, removing from '
                'list.'.format(snap)
            )
            self.snapshots.remove(snap)

    def __excludify(self, condition, exclude, snap, msg=None):
        if condition == True:
            if exclude:
                text = "Removed from actionable list"
                self.__not_actionable(snap)
            else:
                text = "Remains in actionable list"
                self.__actionable(snap)
        else:
            if exclude:
                text = "Remains in actionable list"
                self.__actionable(snap)
            else:
                text = "Removed from actionable list"
                self.__not_actionable(snap)
        if msg:
            self.loggit.debug('{0}: {1}'.format(text, msg))

    def __get_snapshots(self):
        """
        Pull all snapshots into `snapshots` and populate
        `snapshot_info`
        """
        self.all_snapshots = utils.get_snapshot_data(self.client, self.repository)
        for list_item in self.all_snapshots:
            if 'snapshot' in list_item.keys():
                self.snapshots.append(list_item['snapshot'])
                self.snapshot_info[list_item['snapshot']] = list_item
        self.empty_list_check()

    def __map_method(self, ft):
        methods = {
            'age': self.filter_by_age,
            'count': self.filter_by_count,
            'none': self.filter_none,
            'pattern': self.filter_by_regex,
            'period': self.filter_period,
            'state': self.filter_by_state,
        }
        return methods[ft]

    def empty_list_check(self):
        """Raise exception if `snapshots` is empty"""
        if not self.snapshots:
            raise exceptions.NoSnapshots('snapshot_list object is empty.')

    def working_list(self):
        """
        Return the current value of `snapshots` as copy-by-value to prevent list
        stomping during iterations
        """
        # Copy by value, rather than reference to prevent list stomping during
        # iterations
        return self.snapshots[:]

    def _get_name_based_ages(self, timestring):
        """
        Add a snapshot age to `snapshot_info` based on the age as indicated
        by the snapshot name pattern, if it matches `timestring`.  This is
        stored at key ``age_by_name``.

        :arg timestring: An strftime pattern
        """
        # Check for empty list before proceeding here to prevent non-iterable
        # condition
        self.empty_list_check()
        ts = utils.TimestringSearch(timestring)
        for snapshot in self.working_list():
            epoch = ts.get_epoch(snapshot)
            if epoch:
                self.snapshot_info[snapshot]['age_by_name'] = epoch
            else:
                self.snapshot_info[snapshot]['age_by_name'] = None

    def _calculate_ages(self, source='creation_date', timestring=None):
        """
        This method initiates snapshot age calculation based on the given
        parameters.  Exceptions are raised when they are improperly configured.

        Set instance variable `age_keyfield` for use later, if needed.

        :arg source: Source of snapshot age. Can be 'name' or 'creation_date'.
        :arg timestring: An strftime string to match the datestamp in an
            snapshot name. Only used if ``source`` is ``name``.
        """
        if source == 'name':
            self.age_keyfield = 'age_by_name'
            if not timestring:
                raise exceptions.MissingArgument(
                    'source "name" requires the "timestring" keyword argument'
                )
            self._get_name_based_ages(timestring)
        elif source == 'creation_date':
            self.age_keyfield = 'start_time_in_millis'
        else:
            raise ValueError(
                'Invalid source: {0}.  '
                'Must be "name", or "creation_date".'.format(source)
            )

    def _sort_by_age(self, snapshot_list, reverse=True):
        """
        Take a list of snapshots and sort them by date.

        By default, the youngest are first with `reverse=True`, but the oldest
        can be first by setting `reverse=False`
        """
        # Do the age-based sorting here.
        # First, build an temporary dictionary with just snapshot and age
        # as the key and value, respectively
        temp = {}
        for snap in snapshot_list:
            if self.age_keyfield in self.snapshot_info[snap]:
                temp[snap] = self.snapshot_info[snap][self.age_keyfield]
            else:
                msg = (
                    '{0} does not have age key "{1}" in SnapshotList '
                    ' metadata'.format(snap, self.age_keyfield)
                )
                self.__excludify(True, True, snap, msg)

        # If reverse is True, this will sort so the youngest snapshots are
        # first.  However, if you want oldest first, set reverse to False.
        # Effectively, this should set us up to act on everything older than
        # meets the other set criteria.
        # It starts as a tuple, but then becomes a list.
        sorted_tuple = (
            sorted(temp.items(), key=lambda k: k[1], reverse=reverse)
        )
        return [x[0] for x in sorted_tuple]

    def most_recent(self):
        """
        Return the most recent snapshot based on `start_time_in_millis`.
        """
        self.empty_list_check()
        most_recent_time = 0
        most_recent_snap = ''
        for snapshot in self.snapshots:
            snaptime = utils.fix_epoch(
                self.snapshot_info[snapshot]['start_time_in_millis'])
            if snaptime > most_recent_time:
                most_recent_snap = snapshot
                most_recent_time = snaptime
        return most_recent_snap


    def filter_by_regex(self, kind=None, value=None, exclude=False):
        """
        Filter out snapshots not matching the pattern, or in the case of
        exclude, filter those matching the pattern.

        :arg kind: Can be one of: ``suffix``, ``prefix``, ``regex``, or
            ``timestring``. This option defines what kind of filter you will be
            building.
        :arg value: Depends on `kind`. It is the strftime string if `kind` is
            `timestring`. It's used to build the regular expression for other
            kinds.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            snapshots from `snapshots`. If `exclude` is `False`, then only
            matching snapshots will be kept in `snapshots`.
            Default is `False`
        """
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
            regex = settings.regex_map()[kind].format(utils.get_date_regex(value))
        else:
            regex = settings.regex_map()[kind].format(value)

        self.empty_list_check()
        pattern = re.compile(regex)
        for snapshot in self.working_list():
            match = pattern.search(snapshot)
            self.loggit.debug('Filter by regex: Snapshot: {0}'.format(snapshot))
            if match:
                self.__excludify(True, exclude, snapshot)
            else:
                self.__excludify(False, exclude, snapshot)

    def filter_by_age(self, source='creation_date', direction=None,
        timestring=None, unit=None, unit_count=None, epoch=None, exclude=False
        ):
        """
        Remove snapshots from `snapshots` by relative age calculations.

        :arg source: Source of snapshot age. Can be 'name', or 'creation_date'.
        :arg direction: Time to filter, either ``older`` or ``younger``
        :arg timestring: An strftime string to match the datestamp in an
            snapshot name. Only used for snapshot filtering by ``name``.
        :arg unit: One of ``seconds``, ``minutes``, ``hours``, ``days``,
            ``weeks``, ``months``, or ``years``.
        :arg unit_count: The number of ``unit`` (s). ``unit_count`` * ``unit`` will
            be calculated out to the relative number of seconds.
        :arg epoch: An epoch timestamp used in conjunction with ``unit`` and
            ``unit_count`` to establish a point of reference for calculations.
            If not provided, the current time will be used.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            snapshots from `snapshots`. If `exclude` is `False`, then only
            matching snapshots will be kept in `snapshots`.
            Default is `False`
        """
        self.loggit.debug('Starting filter_by_age')
        # Get timestamp point of reference, PoR
        PoR = utils.get_point_of_reference(unit, unit_count, epoch)
        self.loggit.debug('Point of Reference: {0}'.format(PoR))
        if not direction:
            raise exceptions.MissingArgument('Must provide a value for "direction"')
        if direction not in ['older', 'younger']:
            raise ValueError(
                'Invalid value for "direction": {0}'.format(direction)
            )
        self._calculate_ages(source=source, timestring=timestring)
        for snapshot in self.working_list():
            if not self.snapshot_info[snapshot][self.age_keyfield]:
                self.loggit.debug('Removing snapshot {0} for having no age')
                self.snapshots.remove(snapshot)
                continue
            msg = (
                'Snapshot "{0}" age ({1}), direction: "{2}", point of '
                'reference, ({3})'.format(
                    snapshot,
                    utils.fix_epoch(self.snapshot_info[snapshot][self.age_keyfield]),
                    direction,
                    PoR
                )
            )
            # Because time adds to epoch, smaller numbers are actually older
            # timestamps.
            snapshot_age = utils.fix_epoch(
                self.snapshot_info[snapshot][self.age_keyfield])
            if direction == 'older':
                agetest = snapshot_age < PoR
            else: # 'younger'
                agetest = snapshot_age > PoR
            self.__excludify(agetest, exclude, snapshot, msg)

    def filter_by_state(self, state=None, exclude=False):
        """
        Filter out snapshots not matching ``state``, or in the case of exclude,
        filter those matching ``state``.

        :arg state: The snapshot state to filter for. Must be one of
            ``SUCCESS``, ``PARTIAL``, ``FAILED``, or ``IN_PROGRESS``.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            snapshots from `snapshots`. If `exclude` is `False`, then only
            matching snapshots will be kept in `snapshots`.
            Default is `False`
        """
        if state.upper() not in ['SUCCESS', 'PARTIAL', 'FAILED', 'IN_PROGRESS']:
            raise ValueError('{0}: Invalid value for state'.format(state))

        self.empty_list_check()
        for snapshot in self.working_list():
            self.loggit.debug('Filter by state: Snapshot: {0}'.format(snapshot))
            if self.snapshot_info[snapshot]['state'] == state:
                self.__excludify(True, exclude, snapshot)
            else:
                self.__excludify(False, exclude, snapshot)

    def filter_none(self):
        self.loggit.debug('"None" filter selected.  No filtering will be done.')

    def filter_by_count(
            self, count=None, reverse=True, use_age=False,
            source='creation_date', timestring=None, exclude=True
        ):
        """
        Remove snapshots from the actionable list beyond the number `count`,
        sorted reverse-alphabetically by default.  If you set `reverse` to
        `False`, it will be sorted alphabetically.

        The default is usually what you will want. If only one kind of snapshot
        is provided--for example, snapshots matching ``curator-%Y%m%d%H%M%S``--
        then reverse alphabetical sorting will mean the oldest will remain in
        the list, because lower numbers in the dates mean older snapshots.

        By setting `reverse` to `False`, then ``snapshot3`` will be acted on
        before ``snapshot2``, which will be acted on before ``snapshot1``

        `use_age` allows ordering snapshots by age. Age is determined by the
        snapshot creation date (as identified by ``start_time_in_millis``) by
        default, but you can also specify a `source` of ``name``.  The ``name``
        `source` requires the timestring argument.

        :arg count: Filter snapshots beyond `count`.
        :arg reverse: The filtering direction. (default: `True`).
        :arg use_age: Sort snapshots by age.  ``source`` is required in this
            case.
        :arg source: Source of snapshot age. Can be one of ``name``, or
            ``creation_date``. Default: ``creation_date``
        :arg timestring: An strftime string to match the datestamp in a
            snapshot name. Only used if `source` ``name`` is selected.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            snapshots from `snapshots`. If `exclude` is `False`, then only
            matching snapshots will be kept in `snapshots`.
            Default is `True`
        """
        self.loggit.debug('Filtering snapshots by count')
        if not count:
            raise exceptions.MissingArgument('No value for "count" provided')

        # Create a copy-by-value working list
        working_list = self.working_list()

        if use_age:
            self._calculate_ages(source=source, timestring=timestring)
            # Using default value of reverse=True in self._sort_by_age()
            sorted_snapshots = self._sort_by_age(working_list, reverse=reverse)
        else:
            # Default to sorting by snapshot name
            sorted_snapshots = sorted(working_list, reverse=reverse)

        idx = 1
        for snap in sorted_snapshots:
            msg = (
                '{0} is {1} of specified count of {2}.'.format(
                    snap, idx, count
                )
            )
            condition = True if idx <= count else False
            self.__excludify(condition, exclude, snap, msg)
            idx += 1

    def filter_period(
        self, period_type='relative', source='name', range_from=None, 
        range_to=None, date_from=None, date_to=None, date_from_format=None, 
        date_to_format=None, timestring=None, unit=None, 
        week_starts_on='sunday', epoch=None, exclude=False,
        ):
        """
        Match `snapshots` with ages within a given period.
        
        :arg period_type: Can be either ``absolute`` or ``relative``.  Default is
            ``relative``.  ``date_from`` and ``date_to`` are required when using
            ``period_type='absolute'`. ``range_from`` and ``range_to`` are
            required with ``period_type='relative'`.
        :arg source: Source of snapshot age. Can be 'name', or 'creation_date'.
        :arg range_from: How many ``unit`` (s) in the past/future is the origin?
        :arg range_to: How many ``unit`` (s) in the past/future is the end point?
        :arg date_from: The simplified date for the start of the range
        :arg date_to: The simplified date for the end of the range.  If this value
            is the same as ``date_from``, the full value of ``unit`` will be
            extrapolated for the range.  For example, if ``unit`` is ``months``,
            and ``date_from`` and ``date_to`` are both ``2017.01``, then the entire
            month of January 2017 will be the absolute date range.
        :arg date_from_format: The strftime string used to parse ``date_from``
        :arg date_to_format: The strftime string used to parse ``date_to``
        :arg timestring: An strftime string to match the datestamp in an
            snapshot name. Only used for snapshot filtering by ``name``.
        :arg unit: One of ``hours``, ``days``, ``weeks``, ``months``, or
            ``years``.
        :arg week_starts_on: Either ``sunday`` or ``monday``. Default is
            ``sunday``
        :arg epoch: An epoch timestamp used to establish a point of reference
            for calculations. If not provided, the current time will be used.
        :arg exclude: If `exclude` is `True`, this filter will remove matching
            indices from `indices`. If `exclude` is `False`, then only matching
            indices will be kept in `indices`.
            Default is `False`
        """

        self.loggit.debug('Filtering snapshots by period')
        if period_type not in ['absolute', 'relative']:
            raise ValueError(
                'Unacceptable value: {0} -- "period_type" must be either '
                '"absolute" or "relative".'.format(period_type)
            )
        if period_type == 'relative':
            func = utils.date_range
            args = [unit, range_from, range_to, epoch]
            kwgs = { 'week_starts_on': week_starts_on }
            if type(range_from) != type(int()) or type(range_to) != type(int()):
                raise exceptions.ConfigurationError(
                    '"range_from" and "range_to" must be integer values')
        else:
            func = utils.absolute_date_range
            args = [unit, date_from, date_to]
            kwgs = { 
                'date_from_format': date_from_format, 
                'date_to_format': date_to_format 
            }
            for reqd in [date_from, date_to, date_from_format, date_to_format]:
                if not reqd:
                    raise exceptions.ConfigurationError(
                        'Must provide "date_from", "date_to", '
                        '"date_from_format", and "date_to_format" with '
                        'absolute period_type'
                    )
        try:
            start, end = func(*args, **kwgs)
        except Exception as e:
            utils.report_failure(e)
        self._calculate_ages(source=source, timestring=timestring)
        for snapshot in self.working_list():
            if not self.snapshot_info[snapshot][self.age_keyfield]:
                self.loggit.debug('Removing snapshot {0} for having no age')
                self.snapshots.remove(snapshot)
                continue
            age = utils.fix_epoch(self.snapshot_info[snapshot][self.age_keyfield])
            msg = (
                'Snapshot "{0}" age ({1}), period start: "{2}", period '
                'end, ({3})'.format(
                    snapshot,
                    age,
                    start,
                    end
                )
            )
            # Because time adds to epoch, smaller numbers are actually older
            # timestamps.
            inrange = ((age >= start) and (age <= end))
            self.__excludify(inrange, exclude, snapshot, msg)

    def iterate_filters(self, config):
        """
        Iterate over the filters defined in `config` and execute them.



        :arg config: A dictionary of filters, as extracted from the YAML
            configuration file.

        .. note:: `config` should be a dictionary with the following form:
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
        # Make sure we actually _have_ filters to act on
        if not 'filters' in config or len(config['filters']) < 1:
            self.loggit.info('No filters in config.  Returning unaltered object.')
            return

        self.loggit.debug('All filters: {0}'.format(config['filters']))
        for f in config['filters']:
            self.loggit.debug('Top of the loop: {0}'.format(self.snapshots))
            self.loggit.debug('Un-parsed filter args: {0}'.format(f))
            self.loggit.debug('Parsed filter args: {0}'.format(
                    SchemaCheck(
                        f,
                        filters.structure(),
                        'filter',
                        'SnapshotList.iterate_filters'
                    ).result()
                )
            )
            method = self.__map_method(f['filtertype'])
            # Remove key 'filtertype' from dictionary 'f'
            del f['filtertype']
            # If it's a filtertype with arguments, update the defaults with the
            # provided settings.
            self.loggit.debug('Filter args: {0}'.format(f))
            self.loggit.debug('Pre-instance: {0}'.format(self.snapshots))
            method(**f)
            self.loggit.debug('Post-instance: {0}'.format(self.snapshots))
