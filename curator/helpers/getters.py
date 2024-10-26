"""Utility functions that get things"""

import logging
from elasticsearch8 import exceptions as es8exc
from curator.exceptions import (
    ConfigurationError,
    CuratorException,
    FailedExecution,
    MissingArgument,
)


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


def escape_dots(stringval):
    """
    Escape any dots (periods) in ``stringval``.

    Primarily used for ``filter_path`` where dots are indicators of path nesting

    :param stringval: A string, ostensibly an index name

    :type stringval: str

    :returns: ``stringval``, but with any periods escaped with a backslash
    :retval: str
    """
    return stringval.replace('.', r'\.')


def get_alias_actions(oldidx, newidx, aliases):
    """
    :param oldidx: The old index name
    :param newidx: The new index name
    :param aliases: The aliases

    :type oldidx: str
    :type newidx: str
    :type aliases: dict

    :returns: A list of actions suitable for
        :py:meth:`~.elasticsearch.client.IndicesClient.update_aliases` ``actions``
        kwarg.
    :rtype: list
    """
    actions = []
    for alias in aliases.keys():
        actions.append({'remove': {'index': oldidx, 'alias': alias}})
        actions.append({'add': {'index': newidx, 'alias': alias}})
    return actions


def get_data_tiers(client):
    """
    Get all valid data tiers from the node roles of each node in the cluster by
    polling each node

    :param client: A client connection object
    :type client: :py:class:`~.elasticsearch.Elasticsearch`

    :returns: The available data tiers in ``tier: bool`` form.
    :rtype: dict
    """

    def role_check(role, node_info):
        if role in node_info['roles']:
            return True
        return False

    info = client.nodes.info()['nodes']
    retval = {
        'data_hot': False,
        'data_warm': False,
        'data_cold': False,
        'data_frozen': False,
    }
    for node in info:
        for role in ['data_hot', 'data_warm', 'data_cold', 'data_frozen']:
            # This guarantees we don't overwrite a True with a False.
            # We only add True values
            if role_check(role, info[node]):
                retval[role] = True
    return retval


def get_indices(client, search_pattern='_all'):
    """
    Calls :py:meth:`~.elasticsearch.client.CatClient.indices`

    :param client: A client connection object
    :type client: :py:class:`~.elasticsearch.Elasticsearch`

    :returns: The current list of indices from the cluster
    :rtype: list
    """
    logger = logging.getLogger(__name__)
    indices = []
    try:
        # Doing this in two stages because IndexList also calls for these args,
        # and the unit tests need to Mock this call the same exact way.
        resp = client.cat.indices(
            index=search_pattern,
            expand_wildcards='open,closed',
            h='index,status',
            format='json',
        )
    except Exception as err:
        raise FailedExecution(f'Failed to get indices. Error: {err}') from err
    if not resp:
        return indices
    for entry in resp:
        indices.append(entry['index'])
    logger.debug('All indices: %s', indices)
    return indices


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

    :returns: Information about the provided ``snapshot``, a snapshot (or a
        comma-separated list of snapshots). If no snapshot specified, it will
        collect info for all snapshots.  If none exist, an empty :py:class:`dict`
        will be returned.
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


def get_tier_preference(client, target_tier='data_frozen'):
    """Do the tier preference thing in reverse order from coldest to hottest
    Based on the value of ``target_tier``, build out the list to use.

    :param client: A client connection object
    :param target_tier: The target data tier, e.g. data_warm.

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type target_tier: str

    :returns: A suitable tier preference string in csv format
    :rtype: str
    """
    tiermap = {
        'data_content': 0,
        'data_hot': 1,
        'data_warm': 2,
        'data_cold': 3,
        'data_frozen': 4,
    }
    tiers = get_data_tiers(client)
    test_list = []
    for tier in ['data_hot', 'data_warm', 'data_cold', 'data_frozen']:
        if tier in tiers and tiermap[tier] <= tiermap[target_tier]:
            test_list.insert(0, tier)
    if target_tier == 'data_frozen':
        # We're migrating to frozen here. If a frozen tier exists, frozen searchable
        # snapshot mounts should only ever go to the frozen tier.
        if 'data_frozen' in tiers and tiers['data_frozen']:
            return 'data_frozen'
    # If there are no  nodes with the 'data_frozen' role...
    preflist = []
    for key in test_list:
        # This ordering ensures that colder tiers are prioritized
        if key in tiers and tiers[key]:
            preflist.append(key)
    # If all of these are false, then we have no data tiers and must use 'data_content'
    if not preflist:
        return 'data_content'
    # This will join from coldest to hottest as csv string,
    # e.g. 'data_cold,data_warm,data_hot'
    return ','.join(preflist)


def get_write_index(client, alias):
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_alias`

    :param client: A client connection object
    :param alias: An alias name

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type alias: str

    :returns: The the index name associated with the alias that is designated
        ``is_write_index``
    :rtype: str
    """
    try:
        response = client.indices.get_alias(index=alias)
    except Exception as exc:
        raise CuratorException(f'Alias {alias} not found') from exc
    # If there are more than one in the list, one needs to be the write index
    # otherwise the alias is a one to many, and can't do rollover.
    retval = None
    if len(list(response.keys())) > 1:
        for index in list(response.keys()):
            try:
                if response[index]['aliases'][alias]['is_write_index']:
                    retval = index
            except KeyError as exc:
                raise FailedExecution(
                    'Invalid alias: is_write_index not found in 1 to many alias'
                ) from exc
    else:
        # There's only one, so this is it
        retval = list(response.keys())[0]
    return retval


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
    fpath = f'indices.{escape_dots(idx)}.{value}.store.size_in_bytes'
    return client.indices.stats(index=idx, filter_path=fpath)['indices'][idx][value][
        'store'
    ]['size_in_bytes']


def meta_getter(client, idx, get=None):
    """Meta Getter
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_settings` or
    :py:meth:`~.elasticsearch.client.IndicesClient.get_alias`

    :param client: A client connection object
    :param idx: An Elasticsearch index
    :param get: The kind of get to perform, e.g. settings or alias

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type idx: str
    :type get: str

    :returns: The settings from the get call to the named index
    :rtype: dict
    """
    logger = logging.getLogger(__name__)
    acceptable = ['settings', 'alias']
    if not get:
        raise ConfigurationError('"get" can not be a NoneType')
    if get not in acceptable:
        raise ConfigurationError(f'"get" must be one of {acceptable}')
    retval = {}
    try:
        if get == 'settings':
            retval = client.indices.get_settings(index=idx)[idx]['settings']['index']
        elif get == 'alias':
            retval = client.indices.get_alias(index=idx)[idx]['aliases']
    except es8exc.NotFoundError as missing:
        logger.error('Index %s was not found!', idx)
        raise es8exc.NotFoundError from missing
    except KeyError as err:
        logger.error('Key not found: %s', err)
        raise KeyError from err
    # pylint: disable=broad-except
    except Exception as exc:
        logger.error('Exception encountered: %s', exc)
    return retval


def name_to_node_id(client, name):
    """
    Calls :py:meth:`~.elasticsearch.client.NodesClient.info`

    :param client: A client connection object
    :param name: The node ``name``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type name: str

    :returns: The node_id of the node identified by ``name``
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    fpath = 'nodes'
    info = client.nodes.info(filter_path=fpath)
    for node in info['nodes']:
        if info['nodes'][node]['name'] == name:
            logger.debug('Found node_id "%s" for name "%s".', node, name)
            return node
    logger.error('No node_id found matching name: "%s"', name)
    return None


def node_id_to_name(client, node_id):
    """
    Calls :py:meth:`~.elasticsearch.client.NodesClient.info`

    :param client: A client connection object
    :param node_id: The node ``node_id``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type node_id: str

    :returns: The name of the node identified by ``node_id``
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    fpath = f'nodes.{node_id}.name'
    info = client.nodes.info(filter_path=fpath)
    name = None
    if node_id in info['nodes']:
        name = info['nodes'][node_id]['name']
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
    fpath = f'nodes.{node_id}.roles'
    return client.nodes.info(filter_path=fpath)['nodes'][node_id]['roles']


def single_data_path(client, node_id):
    """
    In order for a shrink to work, it should be on a single filesystem, as shards
    cannot span filesystems. Calls :py:meth:`~.elasticsearch.client.NodesClient.stats`

    :param client: A client connection object
    :param node_id: The node ``node_id``

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type node_id: str

    :returns: ``True`` if the node has a single filesystem, else ``False``
    :rtype: bool
    """
    fpath = f'nodes.{node_id}.fs.data'
    response = client.nodes.stats(filter_path=fpath)
    return len(response['nodes'][node_id]['fs']['data']) == 1
