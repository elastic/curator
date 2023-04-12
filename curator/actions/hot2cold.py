"""Snapshot and Restore action classes"""
import logging
from curator import SnapshotList
from curator.helpers.getters import get_alias_actions, get_tier_preference
from curator.helpers.testers import has_lifecycle_name, is_idx_partial, verify_index_list
from curator.helpers.utils import report_failure
from curator.exceptions import (
    ConfigurationError, CuratorException, FailedExecution, SearchableSnapshotException)

class Hot2Cold:
    """Hot to Cold Tier Searchable Snapshot Action Class

    For manually migrating snapshots not associated with ILM from the hot or warm tier to the cold
    tier.
    """

    DEFAULTS = {
        'index_settings': None,
        'ignore_index_settings': ['index.refresh_interval'],
        'wait_for_completion': True,
    }
    NAME = 'hot2cold'         # Frozen is 'hot2frozen'
    PREFIX = 'restored-'      # Frozen is 'partial-'
    STORAGE = 'full_copy'     # Frozen is 'shared_cache'
    TARGET_TIER = 'data_cold' # Frozen is 'data_frozen'

    def __init__(self, ilo, **kwargs):
        """
        :param ilo: An IndexList Object
        :param index_settings: (Optional) Settings that should be added to the index when it is
            mounted. If not set, set the ``_tier_preference`` to the tiers available, coldest
            first.
        :param ignore_index_settings: (Optional, array of strings) Names of settings that should
            be removed from the index when it is mounted.
        :param repository: Repository name.
        :param snapshot: Snapshot name. If none provided, it will use the most recent snapshot from ``repository``.

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type index_settings: dict
        :type ignore_index_settings: list
        :type repository: str
        :type snapshot: str
        """
        self.loggit = logging.getLogger('curator.actions.hot2cold')
        verify_index_list(ilo)
        # Check here and don't bother with the rest of this if there are no
        # indices in the index list.
        ilo.empty_list_check()

        #: The :py:class:`~.curator.indexlist.IndexList` object passed from param ``ilo``
        self.index_list = ilo
        #: The :py:class:`~.elasticsearch.Elasticsearch` client object derived from
        #: :py:attr:`index_list`
        self.client = ilo.client
        #: Object attribute that contains the :py:func:`~.curator.helpers.utils.to_csv` output of
        #: the indices in :py:attr:`index_list`.
        self.indices = ilo

        #: Repository name
        self.repository = None
        #: Snapshot name
        self.snapshot = None

        #: Object attribute that gets the value of ``index_settings``.
        self.index_settings = None
        #: Object attribute that gets the value of ``ignore_index_settings``.
        self.ignore_index_settings = None

        # Parse the kwargs into attributes
        self.assign_kwargs(**kwargs)

        if not self.repository:
            raise ConfigurationError('No repository provided')
        if self.snapshot is None:
            slo = SnapshotList(self.client)
            self.snapshot = slo.most_recent()

    def assign_kwargs(self, **kwargs):
        """
        Assign the kwargs to the attribute of the same name with the passed value or the default
        from DEFAULTS
        """
        # Handy little loop here only adds kwargs that exist in DEFAULTS, or the default value.
        # It ignores any non-relevant kwargs
        for key, value in self.DEFAULTS.items():
            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, value)

    def verify_snap_indices(self):
        """Verify that the provided repository and snapshot match the index list

        """
        try:
            snapdata = self.client.snapshot.get(
                repository=self.repository, snapshot=self.snapshot, index_names=True)
        except Exception as exc:
            raise FailedExecution(
                f'Unable to get snapshot "{self.snapshot}" info from '
                f'repository "{self.repository}". Exception: {exc}'
            ) from exc
        if len(snapdata['snapshots']) != 1:
            raise FailedExecution('Snapshot data has multiple instances')
        snap_indices = snapdata['snapshots'][0]['indices']
        missing = False
        for idx in self.index_list.indices:
            if idx not in snap_indices:
                missing = True
                self.loggit.error('Index "%s" not in snapshot', idx)
        if missing:
            raise FailedExecution('One or more indices not found in snapshot.')

    def action_generator(self):
        """Yield a dict for use in :py:meth:`do_action` and :py:meth:`do_dry_run`

        :returns: A generator object containing the settings necessary to migrate indices from cold
            to frozen
        :rtype: dict
        """
        for idx in self.index_list.indices:
            idx_settings = self.client.indices.get(index=idx)[idx]['settings']['index']
            if has_lifecycle_name(idx_settings):
                self.loggit.critical(
                    'Index %s is associated with an ILM policy and this action will never work on '
                    'an index associated with an ILM policy', idx)
                raise CuratorException(f'Index {idx} is associated with an ILM policy')
            # if is_idx_partial(idx_settings):
            #     self.loggit.critical('Index %s is already in the frozen tier', idx)
            #     raise SearchableSnapshotException('Index is already in frozen tier')
            # snap = idx_settings['store']['snapshot']['snapshot_name']
            # snap_idx = idx_settings['store']['snapshot']['index_name']
            # repo = idx_settings['store']['snapshot']['repository_name']
            aliases = self.client.indices.get(index=idx)[idx]['aliases']

            # prefix = get_frozen_prefix(snap_idx, idx)
            renamed = f'{self.PREFIX}{idx}'

            if not self.index_settings:
                tier_preference = get_tier_preference(
                                    self.client, target_tier=self.TARGET_TIER)
                if not self.TARGET_TIER in tier_preference:
                    self.loggit.warning(
                        'Unable to target preferred data tier (%s)! Tier preference: %s',
                        self.TARGET_TIER, tier_preference
                    )
                self.index_settings = {
                    "routing": {
                        "allocation": {
                            "include": {
                                "_tier_preference": tier_preference
                            }
                        }
                    }
                }
            yield {
                'repository': self.repository, 'snapshot': self.snapshot, 'index': idx,
                'renamed_index': renamed, 'index_settings': self.index_settings,
                'ignore_index_settings': self.ignore_index_settings,
                'storage': self.STORAGE, 'wait_for_completion': self.wait_for_completion,
                'aliases': aliases, 'current_idx': idx
            }

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        for kwargs in self.action_generator():
            aliases = kwargs.pop('aliases')
            current_idx = kwargs.pop('current_idx')
            msg = (
                f'DRY-RUN: {self.NAME}: from snapshot {kwargs["snapshot"]} in repository '
                f'{kwargs["repository"]}, mount index {kwargs["index"]} renamed as '
                f'{kwargs["renamed_index"]} with index settings: {kwargs["index_settings"]} '
                f'and ignoring settings: {kwargs["ignore_index_settings"]}. wait_for_completion: '
                f'{kwargs["wait_for_completion"]}. Restore aliases: {aliases}. Current index '
                f'name: {current_idx}'
            )
            self.loggit.info(msg)

    def verify_mounted(self, newidx):
        """Verify that the new index is mounted as a searchable snapshot

        :param newidx: The new index name
        :type newidx: str

        :returns: Whether the index is properly mounted
        :rtype: bool
        """
        self.loggit.debug('Verifying new index %s is mounted properly...', newidx)
        idx_settings = self.client.indices.get(index=newidx)[newidx]
        exc = f'Index {newidx} not a mounted searchable snapshot'
        retval = False
        try:
            if idx_settings['settings']['index']['store']['type'] == 'snapshot':
                self.loggit.info('Index %s is mounted as a searchable snapshot', newidx)
                retval = True
            else:
                raise SearchableSnapshotException(exc)
        except KeyError as exception:
            raise SearchableSnapshotException(exc) from exception
        if self.STORAGE == 'shared_cache':
            if is_idx_partial(idx_settings['settings']['index']):
                self.loggit.info('Index %s is mounted for frozen tier', newidx)
            else:
                raise SearchableSnapshotException(
                    f'Index {newidx} not mounted in the frozen tier')
        return retval

    def do_action(self):
        """
        Call :py:meth:`~.elasticsearch.client.SearchableSnapshotsClient.mount` to mount the indices
        in :py:attr:`ilo` in the Frozen tier.

        Verify index looks good

        Call :py:meth:`~.elasticsearch.client.IndicesClient.update_aliases` to update each new
        frozen index with the aliases from the old cold-tier index.

        Verify aliases look good.

        Call :py:meth:`~.elasticsearch.client.IndicesClient.delete` to delete the cold tier index.
        """
        ### Verify indices in ILO are in named repo & snapshot
        self.verify_snap_indices()

        try:
            for kwargs in self.action_generator():
                aliases = kwargs.pop('aliases')
                current_idx = kwargs.pop('current_idx')
                newidx = kwargs['renamed_index']
                # Actually do the mount
                self.loggit.debug('Mounting new index %s in cold tier...', newidx)
                self.client.searchable_snapshots.mount(**kwargs)

                ## Verify it's mounted as a searchable snapshot now:
                if not self.verify_mounted(newidx):
                    raise SearchableSnapshotException(
                        f'Index {current_idx} unable to be mounted as {newidx}')
                self.loggit.info(
                    'Successfully mounted %s as a searchable snapshot, identified as %s',
                    current_idx, newidx
                )
                # Update Aliases
                alias_names = aliases.keys()
                if not alias_names:
                    self.loggit.warning('No aliases associated with index %s', current_idx)
                else:
                    self.loggit.debug('Transferring aliases to new index %s', newidx)
                    self.client.indices.update_aliases(
                        actions=get_alias_actions(current_idx, newidx, aliases))
                    verify = self.client.indices.get(index=newidx)[newidx]['aliases'].keys()
                    if alias_names != verify:
                        self.loggit.error(
                            'Alias names do not match! %s does not match: %s', alias_names, verify)
                        raise FailedExecution('Aliases failed to transfer to new index')
                # Clean up old index
                self.loggit.debug('Deleting old index: %s', current_idx)
                self.client.indices.delete(index=current_idx)


        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
