"""Snapshot and Restore action classes"""
import logging
from curator.helpers.getters import get_alias_actions, get_tier_preference, meta_getter
from curator.helpers.testers import has_lifecycle_name, is_idx_partial, verify_index_list
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

    def action_generator(self):
        """Yield a dict for use in :py:meth:`do_action` and :py:meth:`do_dry_run`

        :returns: A generator object containing the settings necessary to migrate indices from cold
            to frozen
        :rtype: dict
        """
        for idx in self.index_list.indices:
            idx_settings = meta_getter(self.client, idx, get='settings')
            self.loggit.debug('Index %s has settings: %s', idx, idx_settings)
            if has_lifecycle_name(idx_settings):
                self.loggit.critical(
                    'Index %s is associated with an ILM policy and this action will never work on '
                    'an index associated with an ILM policy', idx)
                raise CuratorException(f'Index {idx} is associated with an ILM policy')
            if is_idx_partial(idx_settings):
                self.loggit.critical('Index %s is already in the frozen tier', idx)
                raise SearchableSnapshotException('Index is already in frozen tier')

            snap = idx_settings['store']['snapshot']['snapshot_name']
            snap_idx = idx_settings['store']['snapshot']['index_name']
            repo = idx_settings['store']['snapshot']['repository_name']
            msg = f'Index {idx} Snapshot name: {snap}, Snapshot index: {snap_idx}, repo: {repo}'
            self.loggit.debug(msg)

            aliases = meta_getter(self.client, idx, get='alias')

            renamed = f'partial-{idx}'

            if not self.index_settings:
                self.index_settings = {
                    "routing": {
                        "allocation": {
                            "include": {
                                "_tier_preference": get_tier_preference(self.client)
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

    def mount_index(self, newidx, kwargs):
        """
        Call :py:meth:`~.elasticsearch.client.SearchableSnapshotsClient.mount` to mount the indices
        in :py:attr:`ilo` in the Frozen tier.
        """
        try:
            self.loggit.debug('Mounting new index %s in frozen tier...', newidx)
            self.client.searchable_snapshots.mount(**kwargs)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)

    def verify_mount(self, newidx):
        """
        Verify that newidx is a mounted index
        """
        self.loggit.debug('Verifying new index %s is mounted properly...', newidx)
        idx_settings = self.client.indices.get(index=newidx)[newidx]
        if is_idx_partial(idx_settings['settings']['index']):
            self.loggit.info('Index %s is mounted for frozen tier', newidx)
        else:
            report_failure(SearchableSnapshotException(
                f'Index {newidx} not a mounted searchable snapshot'))

    def update_aliases(self, current_idx, newidx, aliases):
        """
        Call :py:meth:`~.elasticsearch.client.IndicesClient.update_aliases` to update each new
        frozen index with the aliases from the old cold-tier index.

        Verify aliases look good.
        """
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
                report_failure(FailedExecution('Aliases failed to transfer to new index'))

    def cleanup(self, current_idx, newidx):
        """
        Call :py:meth:`~.elasticsearch.client.IndicesClient.delete` to delete the cold tier index.
        """
        self.loggit.debug('Deleting old index: %s', current_idx)
        try:
            self.client.indices.delete(index=current_idx)
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
        self.loggit.info(
            'Successfully migrated %s to the frozen tier as %s', current_idx, newidx)

    def do_action(self):
        """
        Do the actions outlined:
        Extract values from generated kwargs
        Mount
        Verify
        Update Aliases
        Cleanup
        """
        for kwargs in self.action_generator():
            aliases = kwargs.pop('aliases')
            current_idx = kwargs.pop('current_idx')
            newidx = kwargs['renamed_index']

            # Mount the index
            self.mount_index(newidx, kwargs)

            # Verify it's mounted as a partial now:
            self.verify_mount(newidx)

            # Update Aliases
            self.update_aliases(current_idx, newidx, aliases)

            # Clean up old index
            self.cleanup(current_idx, newidx)
