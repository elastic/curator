"""Snapshot and Restore action classes"""
import logging
import re
from curator.helpers.getters import get_data_tiers
from curator.helpers.testers import verify_index_list
from curator.helpers.utils import report_failure
from curator.exceptions import CuratorException, FailedExecution, SearchableSnapshotException

class Cold2Frozen:
    """Cold to Frozen Tier Searchable Snapshot Action Class

    For manually migrating snapshots not associated with ILM from the cold tier to the frozen tier.
    """

    DEFAULTS = {
        'index_settings': None,
        'ignore_index_settings': ['index.refresh_interval'],
        'wait_for_completion': True,
    }
    def __init__(self, ilo, **kwargs):
        """
        :param ilo: An IndexList Object
        :param index_settings: (Optional) Settings that should be added to the index when it is
            mounted. If not set, set the ``_tier_preference`` to the tiers available, coldest
            first.
        :param ignore_index_settings: (Optional, array of strings) Names of settings that should
            be removed from the index when it is mounted.
        :param wait_for_completion: Wait for completion before returning.

        :type ilo: :py:class:`~.curator.indexlist.IndexList`
        :type index_settings: dict
        :type ignore_index_settings: list
        :type wait_for_completion: bool
        """
        self.loggit = logging.getLogger('curator.actions.cold2frozen')
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
        #: Object attribute that gets the value of ``index_settings``.
        self.index_settings = None
        #: Object attribute that gets the value of ``ignore_index_settings``.
        self.ignore_index_settings = None
        #: Object attribute that gets the value of param ``wait_for_completion``.
        self.wait_for_completion = None

        # Parse the kwargs into attributes
        self.assign_kwargs(**kwargs)

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

    def get_alias_actions(self, oldidx, newidx, aliases):
        """
        :param oldidx: The old index name
        :param newidx: The new index name
        :param aliases: The aliases

        :type oldidx: str
        :type newidx: str
        :type aliases: dict

        :returns: A list of actions suitable for
            :py:meth:`~.elasticsearch.client.IndicesClient.update_aliases` ``actions`` kwarg.
        :rtype: list
        """
        actions = []
        for alias in aliases.keys():
            actions.append({'remove': {'index': oldidx, 'alias': alias}})
            actions.append({'add': {'index': newidx, 'alias': alias}})
        return actions

    def get_frozen_prefix(self, oldidx, curridx):
        """
        Use regular expression magic to extract the prefix from the current index, and then use
        that with ``partial-`` in front to name the resulting index.

        If there is no prefix, then we just send back ``partial-``

        :param oldidx: The index name before it was mounted in cold tier
        :param curridx: The current name of the index, as mounted in cold tier

        :type oldidx: str
        :type curridx: str

        :returns: The prefix to prepend the index name with for mounting as frozen
        :rtype: str
        """
        pattern = f'^(.*){oldidx}$'
        regexp = re.compile(pattern)
        match = regexp.match(curridx)
        prefix = match.group(1)
        self.loggit.debug('Detected match group for prefix: %s', prefix)
        if not prefix:
            return 'partial-'
        return f'partial-{prefix}'

    def get_tier_preference(self):
        """Do the tier preference thing in reverse order from coldest to hottest

        :returns: A suitable tier preference string in csv format
        :rtype: str
        """
        tiers = get_data_tiers(self.client)
        # We're migrating from cold to frozen here. If a frozen tier exists, frozen ss mounts
        # should only ever go to the frozen tier.
        if 'data_frozen' in tiers and tiers['data_frozen']:
            return 'data_frozen'
        # If there are no  nodes with the 'data_frozen' role...
        preflist = []
        for key in ['data_cold', 'data_warm', 'data_hot']:
            # This ordering ensures that colder tiers are prioritized
            if key in tiers and tiers[key]:
                preflist.append(key)
        # If all of these are false, then we have no data tiers and must use 'data_content'
        if not preflist:
            return 'data_content'
        # This will join from coldest to hottest as csv string, e.g. 'data_cold,data_warm,data_hot'
        return ','.join(preflist)

    def has_lifecycle_name(self, idx_settings):
        """
        :param idx_settings: The settings for an index being tested
        :type idx_settings: dict

        :returns: ``True`` if a lifecycle name exists in settings, else ``False``
        :rtype: bool
        """
        if 'lifecycle' in idx_settings:
            if 'name' in idx_settings['lifecycle']:
                return True
        return False

    def is_idx_partial(self, idx_settings):
        """
        :param idx_settings: The settings for an index being tested
        :type idx_settings: dict

        :returns: ``True`` if store.snapshot.partial exists in settings, else ``False``
        :rtype: bool
        """
        if 'store' in idx_settings:
            if 'snapshot' in idx_settings['store']:
                if 'partial' in idx_settings['store']['snapshot']:
                    if idx_settings['store']['snapshot']['partial']:
                        return True
                    # store.snapshot.partial exists but is False -- Not a frozen tier mount
                    return False
                # store.snapshot exists, but partial isn't there -- Possibly a cold tier mount
                return False
            raise SearchableSnapshotException('Index not a mounted searchable snapshot')
        raise SearchableSnapshotException('Index not a mounted searchable snapshot')

    def action_generator(self):
        """Yield a dict for use in :py:meth:`do_action` and :py:meth:`do_dry_run`

        :returns: A generator object containing the settings necessary to migrate indices from cold
            to frozen
        :rtype: dict
        """
        for idx in self.index_list.indices:
            idx_settings = self.client.indices.get(index=idx)[idx]['settings']['index']
            if self.has_lifecycle_name(idx_settings):
                self.loggit.critical(
                    'Index %s is associated with an ILM policy and this action will never work on '
                    'an index associated with an ILM policy', idx)
                raise CuratorException(f'Index {idx} is associated with an ILM policy')
            if self.is_idx_partial(idx_settings):
                self.loggit.critical('Index %s is already in the frozen tier', idx)
                raise SearchableSnapshotException('Index is already in frozen tier')
            snap = idx_settings['store']['snapshot']['snapshot_name']
            snap_idx = idx_settings['store']['snapshot']['index_name']
            repo = idx_settings['store']['snapshot']['repository_name']
            aliases = self.client.indices.get(index=idx)[idx]['aliases']

            prefix = self.get_frozen_prefix(snap_idx, idx)
            renamed = f'{prefix}{snap_idx}'

            if not self.index_settings:
                self.index_settings = {
                    "routing": {
                        "allocation": {
                            "include": {
                                "_tier_preference": self.get_tier_preference()
                            }
                        }
                    }
                }
            yield {
                'repository': repo, 'snapshot': snap, 'index': snap_idx,
                'renamed_index': renamed, 'index_settings': self.index_settings,
                'ignore_index_settings': self.ignore_index_settings,
                'storage': 'shared_cache', 'wait_for_completion': self.wait_for_completion,
                'aliases': aliases, 'current_idx': idx
            }

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        for kwargs in self.action_generator():
            aliases = kwargs.pop('aliases')
            current_idx = kwargs.pop('current_idx')
            msg = (
                f'DRY-RUN: cold2frozen: from snapshot {kwargs["snapshot"]} in repository '
                f'{kwargs["repository"]}, mount index {kwargs["index"]} renamed as '
                f'{kwargs["renamed_index"]} with index settings: {kwargs["index_settings"]} '
                f'and ignoring settings: {kwargs["ignore_index_settings"]}. wait_for_completion: '
                f'{kwargs["wait_for_completion"]}. Restore aliases: {aliases}. Current index '
                f'name: {current_idx}'
            )
            self.loggit.info(msg)


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
        try:
            for kwargs in self.action_generator():
                aliases = kwargs.pop('aliases')
                current_idx = kwargs.pop('current_idx')
                newidx = kwargs['renamed_index']
                # Actually do the mount
                self.loggit.debug('Mounting new index %s in frozen tier...', newidx)
                self.client.searchable_snapshots.mount(**kwargs)
                # Verify it's mounted as a partial now:
                self.loggit.debug('Verifying new index %s is mounted properly...', newidx)
                idx_settings = self.client.indices.get(index=newidx)[newidx]
                if self.is_idx_partial(idx_settings['settings']['index']):
                    self.loggit.info('Index %s is mounted for frozen tier', newidx)
                else:
                    raise SearchableSnapshotException(
                        f'Index {newidx} not a mounted searchable snapshot')
                # Update Aliases
                alias_names = aliases.keys()
                if not alias_names:
                    self.loggit.warning('No aliases associated with index %s', current_idx)
                else:
                    self.loggit.debug('Transferring aliases to new index %s', newidx)
                    self.client.indices.update_aliases(
                        actions=self.get_alias_actions(current_idx, newidx, aliases))
                    verify = self.client.indices.get(index=newidx)[newidx]['aliases'].keys()
                    if alias_names != verify:
                        self.loggit.error(
                            'Alias names do not match! %s does not match: %s', alias_names, verify)
                        raise FailedExecution('Aliases failed to transfer to new index')
                # Clean up old index
                self.loggit.debug('Deleting old index: %s', current_idx)
                self.client.indices.delete(index=current_idx)
                self.loggit.info(
                    'Successfully migrated %s to the frozen tier as %s', current_idx, newidx)

        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
