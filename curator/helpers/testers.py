"""Utility functions that get things"""
# :pylint disable=
from time import sleep
import logging
from curator.helpers.getters import get_snapshot_data
from curator.exceptions import CuratorException, MissingArgument

### This function is only called by safe_to_snap, which is not necessary in ES 7.16+
def snapshot_in_progress(client, repository=None, snapshot=None):
    """
    Determine whether the provided snapshot in `repository` is ``IN_PROGRESS``.
    If no value is provided for `snapshot`, then check all of them.
    Return `snapshot` if it is found to be in progress, or `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name
    """
    allsnaps = get_snapshot_data(client, repository=repository)
    inprogress = (
        [snap['snapshot'] for snap in allsnaps if 'state' in snap.keys() \
            and snap['state'] == 'IN_PROGRESS']
    )
    if snapshot:
        retval = snapshot if snapshot in inprogress else False
    else:
        if not inprogress:
            retval = False
        elif len(inprogress) == 1:
            retval = inprogress[0]
        else: # This should not be possible
            raise CuratorException(f'More than 1 snapshot in progress: {inprogress}')
    return retval

def find_snapshot_tasks(client):
    """
    Check if there is snapshot activity in the Tasks API.
    Return `True` if activity is found, or `False`

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    retval = False
    tasklist = client.tasks.list()
    for node in tasklist['nodes']:
        for task in tasklist['nodes'][node]['tasks']:
            activity = tasklist['nodes'][node]['tasks'][task]['action']
            if 'snapshot' in activity:
                logger.debug('Snapshot activity detected: %s', activity)
                retval = True
    return retval

### This check is not necessary after ES 7.16 as it is possible to have
### up to 1000 concurrent snapshots
###
### https://www.elastic.co/guide/en/elasticsearch/reference/8.6/snapshot-settings.html
### snapshot.max_concurrent_operations
### (Dynamic, integer) Maximum number of concurrent snapshot operations. Defaults to 1000.
###
### This limit applies in total to all ongoing snapshot creation, cloning, and deletion
### operations. Elasticsearch will reject any operations that would exceed this limit.
def safe_to_snap(client, repository=None, retry_interval=120, retry_count=3):
    """
    Ensure there are no snapshots in progress.  Pause and retry accordingly

    :arg client: An :class:`elasticsearch.Elasticsearch` client object
    :arg repository: The Elasticsearch snapshot repository to use
    :arg retry_interval: Number of seconds to delay betwen retries. Default:
        120 (seconds)
    :arg retry_count: Number of attempts to make. Default: 3
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    for count in range(1, retry_count+1):
        in_progress = snapshot_in_progress(
            client, repository=repository
        )
        ongoing_task = find_snapshot_tasks(client)
        if in_progress or ongoing_task:
            if in_progress:
                logger.info('Snapshot already in progress: %s', in_progress)
            elif ongoing_task:
                logger.info('Snapshot activity detected in Tasks API')
            logger.info('Pausing %s seconds before retrying...', retry_interval)
            sleep(retry_interval)
            logger.info('Retry %s of %s', count, retry_count)
        else:
            return True
    return False