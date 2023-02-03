"""Utility functions that get things"""
# :pylint disable=
from elasticsearch8 import exceptions as es8exc
from curator.exceptions import CuratorException, FailedExecution, MissingArgument

def get_repository(client, repository=''):
    """
    :param client: An :class:`elasticsearch.Elasticsearch` client object
    :param repository: The Elasticsearch snapshot repository to use

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
    :param client: An :class:`elasticsearch.Elasticsearch` client object
    :param repository: The Elasticsearch snapshot repository to use
    :param snapshot: The snapshot name, or a comma-separated list of snapshots

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

    :param client: An :class:`elasticsearch.Elasticsearch` client object
    :param repository: The Elasticsearch snapshot repository to use

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