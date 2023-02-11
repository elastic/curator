"""Utility functions that get things"""
# :pylint disable=
import logging
from elasticsearch8 import exceptions as es8exc
from curator.exceptions import CuratorException, FailedExecution, MissingArgument

def byte_size(num, suffix='B'):
    """
    :param num: The number of byte
    :param suffix: An arbitrary suffix, like ``Bytes``

    :type num: int
    :type suffix: str

    :returns: A formatted string indicating the size in bytes, with the proper unit,
        e.g. KB, MB, GB, TB, etc.
    :rtype: float
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return f'{num:3.1f}{unit}{suffix}'
        num /= 1024.0
    return f'{num:.1f}Y{suffix}'

def get_indices(client):
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_settings`

    :param client: A client connection object
    :type client: :py:class:`~.elasticsearch.Elasticsearch`

    :returns: The current list of indices from the cluster
    :rtype: list
    """
    logger = logging.getLogger(__name__)
    try:
        indices = list(client.indices.get_settings(index='*', expand_wildcards='open,closed'))
        logger.debug('All indices: %s', indices)
        return indices
    except Exception as err:
        raise FailedExecution(f'Failed to get indices. Error: {err}') from err

def get_repository(client, repository=''):
    """
    Calls :py:meth:`~.elasticsearch.client.SnapshotClient.get_repository`

    :param client: A client connection object
    :param repository: The Elasticsearch snapshot repository to use

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type repository: str

    :returns: Configuration information for ``repository``.
    :rtype: dict
    """
    try:
        return client.snapshot.get_repository(name=repository)
    except (es8exc.TransportError, es8exc.NotFoundError) as err:
        msg = (
            f'Unable to get repository {repository}.  Error: {err} Check Elasticsearch '
            f'logs for more information.'
        )
        raise CuratorException(msg) from err

def get_snapshot(client, repository=None, snapshot=''):
    """
    Calls :py:meth:`~.elasticsearch.client.SnapshotClient.get`

    :param client: A client connection object
    :param repository: The Elasticsearch snapshot repository to use
    :param snapshot: The snapshot name, or a comma-separated list of snapshots

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type repository: str
    :type snapshot: str

    :returns: Information about the provided ``snapshot``, a snapshot (or a comma-separated list of
        snapshots). If no snapshot specified, it will collect info for all snapshots.  If none
        exist, an empty :py:class:`dict` will be returned.
    :rtype: dict
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    snapname = '*' if snapshot == '' else snapshot
    try:
        return client.snapshot.get(repository=repository, snapshot=snapshot)
    except (es8exc.TransportError, es8exc.NotFoundError) as err:
        msg = (
            f'Unable to get information about snapshot {snapname} from repository: '
            f'{repository}.  Error: {err}'
        )
        raise FailedExecution(msg) from err

def get_snapshot_data(client, repository=None):
    """
    Get all snapshots from repository and return a list.
    Calls :py:meth:`~.elasticsearch.client.SnapshotClient.get`

    :param client: A client connection object
    :param repository: The Elasticsearch snapshot repository to use

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type repository: str

    :returns: The list of all snapshots from ``repository``
    :rtype: list
    """
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    try:
        return client.snapshot.get(repository=repository, snapshot="*")['snapshots']
    except (es8exc.TransportError, es8exc.NotFoundError) as err:
        msg = (
            f'Unable to get snapshot information from repository: '
            f'{repository}. Error: {err}'
        )
        raise FailedExecution(msg) from err

def get_write_index(client, alias):
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_alias`

    :param client: A client connection object
    :param alias: An alias name

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type alias: str

    :returns: The the index name associated with the alias that is designated ``is_write_index``
    :rtype: str
    """
    try:
        response = client.indices.get_alias(index=alias)
    except Exception as exc:
        raise CuratorException(f'Alias {alias} not found') from exc
    # If there are more than one in the list, one needs to be the write index
    # otherwise the alias is a one to many, and can't do rollover.
    if len(list(response.keys())) > 1:
        for index in list(response.keys()):
            try:
                if response[index]['aliases'][alias]['is_write_index']:
                    return index
            except KeyError as exc:
                raise FailedExecution(
                    'Invalid alias: is_write_index not found in 1 to many alias') from exc
    else:
        # There's only one, so this is it
        return list(response.keys())[0]

def index_size(client, idx, value='total'):
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.stats`

    :param client: A client connection object
    :param idx: An index name
    :param value: One of either ``primaries`` or ``total``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type idx: str
    :type value: str

    :returns: The sum of either ``primaries`` or ``total`` shards for index ``idx``
    :rtype: integer
    """
    return client.indices.stats(index=idx)['indices'][idx][value]['store']['size_in_bytes']

def name_to_node_id(client, name):
    """
    Calls :py:meth:`~.elasticsearch.client.NodesClient.stats`

    :param client: A client connection object
    :param name: The node ``name``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type name: str

    :returns: The node_id of the node identified by ``name``
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    stats = client.nodes.stats()
    for node in stats['nodes']:
        if stats['nodes'][node]['name'] == name:
            logger.debug('Found node_id "%s" for name "%s".', node, name)
            return node
    logger.error('No node_id found matching name: "%s"', name)
    return None

def node_id_to_name(client, node_id):
    """
    Calls :py:meth:`~.elasticsearch.client.NodesClient.stats`

    :param client: A client connection object
    :param node_id: The node ``node_id``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type node_id: str

    :returns: The name of the node identified by ``node_id``
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    stats = client.nodes.stats()
    name = None
    if node_id in stats['nodes']:
        name = stats['nodes'][node_id]['name']
    else:
        logger.error('No node_id found matching: "%s"', node_id)
    logger.debug('Name associated with node_id "%s": %s', node_id, name)
    return name

def node_roles(client, node_id):
    """
    Calls :py:meth:`~.elasticsearch.client.NodesClient.info`

    :param client: A client connection object
    :param node_id: The node ``node_id``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type node_id: str

    :returns: The list of roles assigned to the node identified by ``node_id``
    :rtype: list
    """
    return client.nodes.info()['nodes'][node_id]['roles']

def single_data_path(client, node_id):
    """
    In order for a shrink to work, it should be on a single filesystem, as shards cannot span
    filesystems. Calls :py:meth:`~.elasticsearch.client.NodesClient.stats`

    :param client: A client connection object
    :param node_id: The node ``node_id``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type node_id: str

    :returns: ``True`` if the node has a single filesystem, else ``False``
    :rtype: bool
    """
    return len(client.nodes.stats()['nodes'][node_id]['fs']['data']) == 1
