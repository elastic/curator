from .utils import *
import logging
logger = logging.getLogger(__name__)

def create_snapshot(client, indices='_all', name=None,
                    prefix='curator-', repository='',
                    ignore_unavailable=False, include_global_state=True,
                    partial=False, wait_for_completion=True, request_timeout=21600):
    """
    Create a snapshot of provided indices (or ``_all``) that are open.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to snapshot. Default is ``_all``
    :arg name: What to name the snapshot. `prefix` +
        datestamp if omitted.
    :arg prefix: Override the default with this value. Defaults to
        ``curator-``
    :arg repository: The Elasticsearch snapshot repository to use
    :arg wait_for_completion: Wait (or not) for the operation
        to complete before returning.  (default: `True`)
    :type wait_for_completion: bool
    :arg ignore_unavailable: Ignore unavailable shards/indices.
        (default: `False`)
    :type ignore_unavailable: bool
    :arg include_global_state: Store cluster global state with snapshot.
        (default: `True`)
    :type include_global_state: bool
    :arg partial: Do not fail if primary shard is unavailable. (default:
        `False`)
    :type partial: bool
    :rtype bool:
    """
    # Return True if it is skipped
    if not repository:
        logger.error('Missing required repository parameter')
        return False
    if not indices == '_all':
        indices = prune_closed(client, indices)
    if not indices:
        logger.error("No indices provided.")
        return False
    repo_access = (1, 4, 0)
    version_number = get_version(client)
    if version_number >= repo_access:
        try:
            nodes = client.snapshot.verify_repository(repository=repository)['nodes']
            logger.debug('Nodes with verified repository access: {0}'.format(nodes))
        except Exception:
            logger.error('Failed to verify all nodes have repository access.')
            return False
    body=create_snapshot_body(indices, ignore_unavailable=ignore_unavailable,
                                include_global_state=include_global_state,
                                partial=partial)
    name = name if name else prefix + datetime.utcnow().strftime('%Y%m%d%H%M%S')
    logger.info("Snapshot name: {0}".format(name))
    all_snaps = get_snapshots(client, repository=repository)
    if name in all_snaps:
        logger.error("A snapshot with name '{0}' already exists.".format(name))
        return False
    try:
        client.snapshot.create(repository=repository, snapshot=name, body=body,
                                wait_for_completion=wait_for_completion,
                                request_timeout=request_timeout)
        return True
    except elasticsearch.TransportError:
        logger.error("Client raised a TransportError.")
        return False

def delete_snapshot(client, snapshot=None, repository=None):
    """
    Delete a single snapshot from a given repository by name

    :arg client: The Elasticsearch client connection
    :arg snapshot: The snapshot name
    :arg repository: The Elasticsearch snapshot repository to use
    """
    if not repository:
        logger.error('Missing required repository parameter')
        return False
    if not snapshot:
        logger.error('Missing required snapshot parameter')
        return False
    if check_csv(snapshot):
        logger.error('Cannot delete multiple snapshots at once.  CSV value or list detected: {0}'.format(snapshot))
        return False
    try:
        client.snapshot.delete(repository=repository, snapshot=snapshot)
        return True
    except elasticsearch.RequestError:
        logger.error("Unable to delete snapshot {0} from repository {1}.  Check logs for more information.".format(snapshot, repository))
        return False
