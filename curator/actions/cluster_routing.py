"""Cluster Routing action class"""
import logging
# pylint: disable=import-error
from curator.helpers.testers import verify_client_object
from curator.helpers.utils import report_failure
from curator.helpers.waiters import wait_for_it

class ClusterRouting:
    """ClusterRouting Action Class"""
    def __init__(
            self, client, routing_type=None, setting=None, value=None, wait_for_completion=False,
            wait_interval=9, max_wait=-1
    ):
        """
        For now, the cluster routing settings are hardcoded to be ``transient``

        :param client: A client connection object
        :param routing_type: Type of routing to apply. Either ``allocation`` or ``rebalance``
        :param setting: Currently, the only acceptable value for ``setting`` is ``enable``. This is
            here in case that changes.
        :param value: Used only if ``setting`` is ``enable``. Semi-dependent on ``routing_type``.
            Acceptable values for ``allocation`` and ``rebalance`` are ``all``, ``primaries``, and
            ``none`` (string, not :py:class:`None`). If ``routing_type`` is ``allocation``, this
            can also be ``new_primaries``, and if ``rebalance``, it can be ``replicas``.
        :param wait_for_completion: Wait for completion before returning.
        :param wait_interval: Seconds to wait between completion checks.
        :param max_wait: Maximum number of seconds to ``wait_for_completion``

        :type client: :py:class:`~.elasticsearch.Elasticsearch`
        :type routing_type: str
        :type setting: str
        :type value: str
        :type wait_for_completion: bool
        :type wait_interval: int
        :type max_wait: int
        """
        verify_client_object(client)
        #: An :py:class:`~.elasticsearch.Elasticsearch` client object
        self.client = client
        self.loggit = logging.getLogger('curator.actions.cluster_routing')
        #: Object attribute that gets the value of param ``wait_for_completion``
        self.wfc = wait_for_completion
        #: Object attribute that gets the value of param ``wait_interval``
        self.wait_interval = wait_interval
        #: Object attribute that gets the value of param ``max_wait``. How long in seconds to
        #: :py:attr:`wfc` before returning with an exception. A value of ``-1`` means wait forever.
        self.max_wait = max_wait

        if setting != 'enable':
            raise ValueError(f'Invalid value for "setting": {setting}.')
        if routing_type == 'allocation':
            if value not in ['all', 'primaries', 'new_primaries', 'none']:
                raise ValueError(
                    f'Invalid "value": {value} with "routing_type":{routing_type}.')
        elif routing_type == 'rebalance':
            if value not in ['all', 'primaries', 'replicas', 'none']:
                raise ValueError(
                    f'Invalid "value": {value} with "routing_type": {routing_type}.')
        else:
            raise ValueError(f'Invalid value for "routing_type":{routing_type}.')
        bkey = f'cluster.routing.{routing_type}.{setting}'
        #: Populated at instance creation time. Value is built from the passed values from params
        #: ``routing_type`` and ``setting``, e.g. ``cluster.routing.routing_type.setting``
        self.settings = {bkey : value}

    def do_dry_run(self):
        """Log what the output would be, but take no action."""
        self.loggit.info('DRY-RUN MODE.  No changes will be made.')
        msg = f'DRY-RUN: Update cluster routing transient settings: {self.settings}'
        self.loggit.info(msg)

    def do_action(self):
        """
        :py:meth:`~.elasticsearch.client.ClusterClient.put_settings` to the cluster with
        :py:attr:`settings`.
        """
        self.loggit.info('Updating cluster settings: %s', self.settings)
        try:
            self.client.cluster.put_settings(transient=self.settings)
            if self.wfc:
                self.loggit.debug(
                    'Waiting for shards to complete routing and/or rebalancing'
                )
                wait_for_it(
                    self.client, 'cluster_routing',
                    wait_interval=self.wait_interval, max_wait=self.max_wait
                )
        # pylint: disable=broad-except
        except Exception as err:
            report_failure(err)
