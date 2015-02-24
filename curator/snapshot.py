def get_repository(client, repository=''):
    """
    Return configuration information for the indicated repository.

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :rtype: dict
    """
    try:
        return client.snapshot.get_repository(repository=repository)
    except elasticsearch.NotFoundError as e:
        logger.error("Repository {0} not found.  Error: {1}".format(repository, e))
        return None

### Single snapshot information
def get_snapshot(client, repository='', snapshot=''):
    """
    Return information about a snapshot (or a comma-separated list of snapshots)

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot: The snapshot name, or a comma-separated list of snapshots
    :rtype: dict
    """
    try:
        return client.snapshot.get(repository=repository, snapshot=snapshot)
    except elasticsearch.NotFoundError as e:
        logger.error("Snapshot or repository {0} not found.  Error: {1}".format(snapshot, e))
        return None

## Snapshot information
### List of matching snapshots
def get_snaplist(client, repository='', snapshot_prefix='curator-'):
    """
    Get ``_all`` snapshots containing ``snapshot_prefix`` from repository and
    return a list.

    :arg client: The Elasticsearch client connection
    :arg repository: The Elasticsearch snapshot repository to use
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    :rtype: list of strings
    """
    retval = []
    try:
        allsnaps = client.snapshot.get(repository=repository, snapshot="_all")['snapshots']
        snaps = [snap['snapshot'] for snap in allsnaps if 'snapshot' in snap.keys()]
        if snapshot_prefix:
            snapshot_prefix = '.' + snapshot_prefix if snapshot_prefix[0] == '*' else snapshot_prefix
        regex = "^" + snapshot_prefix + ".*" + "$"
        pattern = re.compile(regex)
        return list(filter(lambda x: pattern.search(x), snaps))
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
    return retval

def create_snapshot_body(indices, ignore_unavailable=False,
                         include_global_state=True, partial=False, **kwargs):
    """
    Create the request body for creating a snapshot from the provided
    arguments.

    :arg indices: A single index, or list of indices to snapshot.
    :arg ignore_unavailable: Boolean. Ignore unavailable shards/indices.
        Default is `False`
    :arg include_global_state: Boolean. Store cluster global state with snapshot.
        Default is `True`
    :arg partial: Boolean. Do not fail if primary shard is unavailable. Default
        is `False`
    """
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if indices == '_all':
        body["indices"] = indices
    else:
        if type(indices) is not type(list()):   # in case of a single value passed
            indices = [indices]
        body["indices"] = ','.join(sorted(indices))
    return body

### Create a snapshot
def create_snapshot(client, indices='_all', snapshot_name=None,
                    snapshot_prefix='curator-', repository='',
                    ignore_unavailable=False, include_global_state=True,
                    partial=False, wait_for_completion=True, **kwargs):
    """
    Create a snapshot of provided indices (or ``_all``) that are open.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to snapshot. Default is ``_all``
    :arg snapshot_name: What to name the snapshot. ``snapshot_prefix`` +
        datestamp if omitted.
    :arg snapshot_prefix: Override the default with this value. Defaults to
        ``curator-``
    :arg repository: The Elasticsearch snapshot repository to use
    :arg wait_for_completion: Wait (or not) for the operation
        to complete before returning.  Waits by default, i.e. Default is
        `True`
    :type wait_for_completion: bool
    :arg ignore_unavailable: Ignore unavailable shards/indices.
        Default is `False`
    :type ignore_unavailable: bool
    :arg include_global_state: Store cluster global state with snapshot.
        Default is `True`
    :type include_global_state: bool
    :arg partial: Do not fail if primary shard is unavailable. Default
        is `False`
    :type partial: bool
    """
    # Return True if it is skipped
    if not repository:
        logger.error("Unable to create snapshot. Repository name not provided.")
        return True
    try:
        if not indices == '_all':
            if type(indices) == type(list()):
                indices = [i for i in indices if not index_closed(client, i)]
            else:
                indices = indices if not index_closed(client, indices) else ''
        body=create_snapshot_body(indices, ignore_unavailable=ignore_unavailable, include_global_state=include_global_state, partial=partial)
        datestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        snapshot_name = snapshot_name if snapshot_name else snapshot_prefix + datestamp
        logger.info("Snapshot name: {0}".format(snapshot_name))
        all_snaps = get_snaplist(client, repository=repository, snapshot_prefix=snapshot_prefix)
        if not snapshot_name in all_snaps and len(indices) > 0:
            try:
                client.snapshot.create(repository=repository, snapshot=snapshot_name, body=body, wait_for_completion=wait_for_completion)
            except elasticsearch.TransportError as e:
                logger.error("Client raised a TransportError.  Error: {0}".format(e))
                return True
        elif len(indices) == 0:
            logger.warn("No indices provided.")
            return True
        else:
            logger.info("Skipping: A snapshot with name '{0}' already exists.".format(snapshot_name))
            return True
    except elasticsearch.RequestError as e:
        logger.error("Unable to create snapshot {0}.  Error: {1} Check logs for more information.".format(snapshot_name, e))
        return True

### Delete a snapshot
def delete_snapshot(client, snap, **kwargs):
    """
    Delete a snapshot (or comma-separated list of snapshots)

    :arg client: The Elasticsearch client connection
    :arg snap: The snapshot name
    :arg repository: The Elasticsearch snapshot repository to use
    """
    if not "repository" in kwargs:
        logger.error("Repository information omitted. Must specify repository to delete snapshot.")
    else:
        repository = kwargs["repository"]
    try:
        client.snapshot.delete(repository=repository, snapshot=snap)
    except elasticsearch.RequestError as e:
        logger.error("Unable to delete snapshot {0}.  Error: {1} Check logs for more information.".format(snap, e))
