"""Object builder"""

# pylint: disable=W0718,R0902,R0912,R0913,R0914,R0917
import typing as t
import logging
import sys

from es_client.builder import Builder
from es_client.exceptions import FailedValidation
from es_client.helpers.schemacheck import SchemaCheck
from es_client.helpers.utils import prune_nones
from voluptuous import Schema

from curator import IndexList, SnapshotList
from curator.debug import debug
from curator.actions import (
    Alias,
    Allocation,
    Close,
    ClusterRouting,
    CreateIndex,
    DeleteIndices,
    DeleteSnapshots,
    ForceMerge,
    IndexSettings,
    Open,
    Reindex,
    Replicas,
    Restore,
    Rollover,
    Rotate,
    Setup,
    Shrink,
    Snapshot,
    Status,
)
from curator.defaults.settings import VERSION_MAX, VERSION_MIN, snapshot_actions
from curator.exceptions import ConfigurationError, NoIndices, NoSnapshots
from curator.helpers.testers import validate_filters
from curator.validators import options
from curator.validators.filter_functions import validfilters

logger = logging.getLogger(__name__)

CLASS_MAP = {
    "alias": Alias,
    "allocation": Allocation,
    "close": Close,
    "cluster_routing": ClusterRouting,
    "create_index": CreateIndex,
    "delete_indices": DeleteIndices,
    "delete_snapshots": DeleteSnapshots,
    "forcemerge": ForceMerge,
    "index_settings": IndexSettings,
    "open": Open,
    "reindex": Reindex,
    "replicas": Replicas,
    "restore": Restore,
    "rollover": Rollover,
    "shrink": Shrink,
    "snapshot": Snapshot,
    "rotate": Rotate,
    "setup": Setup,
    "status": Status,
}

EXCLUDED_OPTIONS = [
    "ignore_empty_list",
    "timeout_override",
    "continue_if_exception",
    "disable_action",
]


class CLIAction:
    """
    Unified class for all CLI singleton actions
    """

    def __init__(
        self, action, configdict, option_dict, filter_list, ignore_empty_list, **kwargs
    ):
        """Class setup
        :param action: The action name.
        :param configdict: ``dict`` containing everything needed for
            :py:class:`~.es_client.builder.Builder` to build an
            :py:class:`~.elasticsearch.Elasticsearch` client object.
        :param option_dict: Options for ``action``.
        :param filter_list: Filters to select indices for ``action``.
        :param ignore_empty_list: Exit ``0`` even if filters result in no indices
            for ``action``.
        :param kwargs: Other keyword args to pass to ``action``.

        :type action: str
        :type configdict: dict
        :type option_dict: dict
        :type filter_list: list
        :type ignore_empty_list: bool
        :type kwargs: dict
        """
        self.filters = []
        self.action = action
        self.repository = kwargs['repository'] if 'repository' in kwargs else None
        if action[:5] != 'show_':  # Ignore CLASS_MAP for show_indices/show_snapshots
            try:
                self.action_class = CLASS_MAP[action]
            except KeyError:
                logger.critical('Action must be one of %s', list(CLASS_MAP.keys()))
            self.check_options(option_dict)
        else:
            self.options = option_dict

        self.search_pattern = self.options.pop('search_pattern', '*')
        self.include_datastreams = self.options.pop('include_datastreams', True)
        self.include_hidden = self.options.pop('include_hidden', False)
        self.include_kibana = self.options.pop('include_kibana', False)
        self.include_system = self.options.pop('include_system', False)

        # Extract allow_ilm_indices so it can be handled separately.
        if "allow_ilm_indices" in self.options:
            self.allow_ilm = self.options.pop("allow_ilm_indices")
        else:
            self.allow_ilm = False
        if action == 'alias':
            debug.lv5('ACTION = ALIAS')
            self.alias = {
                "name": option_dict["name"],
                "extra_settings": option_dict["extra_settings"],
                "wini": (
                    kwargs["warn_if_no_indices"]
                    if "warn_if_no_indices" in kwargs
                    else False
                ),
            }
            for k in ["add", "remove"]:
                if k in kwargs:
                    self.alias[k] = {}
                    self.check_filters(kwargs[k], loc="alias singleton", key=k)
                    self.alias[k]["filters"] = self.filters
                    if self.allow_ilm:
                        self.alias[k]["filters"].append({"filtertype": "ilm"})
        # No filters for these actions
        elif action in ["cluster_routing", "create_index", "rollover"]:
            self.action_kwargs = {}
            if action == 'rollover':
                debug.lv5('rollover option_dict = %s', option_dict)
        else:
            self.check_filters(filter_list)
        try:
            builder = Builder(
                configdict=configdict, version_max=VERSION_MAX, version_min=VERSION_MIN
            )
            builder.connect()
        # pylint: disable=broad-except
        except Exception as exc:
            raise ConfigurationError(
                f"Unable to connect to Elasticsearch as configured: {exc}"
            ) from exc
        # If we're here, we'll see the output from GET http(s)://hostname.tld:PORT
        debug.lv5('Connection result: %s', builder.client.info())
        self.client = builder.client
        self.ignore = ignore_empty_list

        self.list_object = self.get_list_object()

    def prune_excluded(self, option_dict):
        """Prune excluded options"""
        for k in list(option_dict.keys()):
            if k in EXCLUDED_OPTIONS:
                del option_dict[k]
        return option_dict

    def check_options(self, option_dict):
        """Validate provided options"""
        try:
            debug.lv5('Validating provided options: %s', option_dict)
            # Kludgy work-around to needing 'repository' in options for these actions
            # but only to pass the schema check.  It's removed again below.
            if self.action in ["delete_snapshots", "restore"]:
                option_dict["repository"] = self.repository
            _ = SchemaCheck(
                prune_nones(option_dict),
                options.get_schema(self.action),
                "options",
                f'{self.action} singleton action "options"',
            ).result()
            self.options = self.prune_excluded(_)
            # Remove this after the schema check, as the action class won't need it as an arg
            if self.action in ["delete_snapshots", "restore"]:
                del self.options["repository"]
        except FailedValidation as exc:
            logger.critical('Unable to parse options: %s', exc)
            sys.exit(1)

    def check_filters(self, filter_dict, loc="singleton", key="filters"):
        """Validate provided filters"""
        try:
            debug.lv5('Validating provided filters: %s', filter_dict)
            _ = SchemaCheck(
                filter_dict,
                Schema(validfilters(self.action, location=loc)),
                key,
                f'{self.action} singleton action "{key}"',
            ).result()
            self.filters = validate_filters(self.action, _)
        except FailedValidation as exc:
            logger.critical('Unable to parse filters: %s', exc)
            sys.exit(1)

    def do_filters(self):
        """Actually run the filters"""
        debug.lv3('Running filters and testing for empty list object')
        if not self.allow_ilm and not self.action in [
            'delete_snapshots',
            'restore',
            'show_snapshots',
        ]:
            self.filters.append({'filtertype': 'ilm', 'exclude': True})
        try:
            self.list_object.iterate_filters({"filters": self.filters})
            self.list_object.empty_list_check()
        except (NoIndices, NoSnapshots) as exc:
            otype = "index" if isinstance(exc, NoIndices) else "snapshot"
            if self.ignore:
                logger.info('Singleton action not performed: empty %s list', otype)
                sys.exit(0)
            else:
                logger.error('Singleton action failed due to empty %s list', otype)
                sys.exit(1)

    def get_list_object(self) -> t.Union[IndexList, SnapshotList]:
        """Get either a SnapshotList or IndexList object"""
        if self.action in snapshot_actions() or self.action == 'show_snapshots':
            return SnapshotList(self.client, repository=self.repository)
        return IndexList(
            self.client,
            search_pattern=self.search_pattern,
            include_datastreams=self.include_datastreams,
            include_hidden=self.include_hidden,
            include_kibana=self.include_kibana,
            include_system=self.include_system,
        )

    def get_alias_obj(self):
        """Get the Alias object"""
        action_obj = Alias(
            name=self.alias["name"], extra_settings=self.alias["extra_settings"]
        )
        for k in ["remove", "add"]:
            if k in self.alias:
                msg = (
                    f"{'Add' if k == 'add' else 'Remov'}ing matching indices "
                    f'{"to" if k == "add" else "from"} alias "{self.alias["name"]}"'
                )
                debug.lv4(msg)
                self.alias[k]['ilo'] = IndexList(
                    self.client,
                    search_pattern=self.search_pattern,
                    include_hidden=self.include_hidden,
                )
                self.alias[k]['ilo'].iterate_filters(
                    {'filters': self.alias[k]['filters']}
                )
                fltr = getattr(action_obj, k)
                fltr(self.alias[k]["ilo"], warn_if_no_indices=self.alias["wini"])
        return action_obj

    def do_singleton_action(self, dry_run=False):
        """Execute the (ostensibly) completely ready to run action"""
        debug.lv3('Doing the singleton "%s" action here.', self.action)
        try:
            if self.action == "alias":
                action_obj = self.get_alias_obj()
            elif self.action in ["cluster_routing", "create_index", "rollover"]:
                action_obj = self.action_class(self.client, **self.options)
            elif self.action in ["setup", "rotate", "status"]:
                self.logger.debug(
                    f"Declaring Deepfreeze action object with options: {self.options}"
                )
                action_obj = self.action_class(self.client, **self.options)
                self.logger.debug("Deepfreeze action object declared")
            else:
                self.get_list_object()
                self.do_filters()
                debug.lv5('OPTIONS = %s', self.options)
                action_obj = self.action_class(self.list_object, **self.options)
            if dry_run:
                action_obj.do_dry_run()
            else:
                action_obj.do_action()
        except NoIndices:  # Speficically to address #1704
            if not self.ignore:
                logger.critical('No indices in list after filtering. Exiting.')
                sys.exit(1)
            logger.info('No indices in list after filtering. Skipping action.')
        except Exception as exc:
            logger.critical(
                'Failed to complete action: %s. Exception: %s', self.action, exc
            )
            sys.exit(1)
        logger.info('"%s" action completed.', self.action)
        sys.exit(0)
