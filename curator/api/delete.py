from .utils import *
import elasticsearch

import logging
logger = logging.getLogger(__name__)

def delete_indices(client, indices):
    """
    Delete the indicated indices, including closed indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype bool:
    """
    indices = ensure_list(indices)
    try:
        client.indices.delete(index=to_csv(indices))
        return True
    except:
        logger.error("Error deleting indices.  Check logs for more information.")
        return False

def delete_by_space(client, indices, disk_space=None):
    """
    Delete from the provide list of indices based on space consumed, sorted
    alphabetically.  If only one kind of index is provided--for example, indices
    matching logstash-%Y.%m.%d--then alphabetically will mean the oldest get
    deleted first.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg disk_space: Delete indices over *n* gigabytes, alphabetically sorted.
    :rtype: bool
    """

    if not disk_space:
        logger.error("Mising value for disk_space.")
        return False

    disk_usage = 0.0
    disk_limit = disk_space * 2**30
    delete_list = []

    not_closed = [i for i in indices if not index_closed(client, i)]
    # Because we're building a csv list of indices to pass, we need to ensure
    # that we actually have at least one index before calling
    # client.indices.status, otherwise the call will match _all indices, which
    # is very bad.
    # See https://github.com/elasticsearch/curator/issues/254
    logger.debug('List of indices found: {0}'.format(not_closed))
    if not_closed:

        stats = client.indices.status(index=to_csv(not_closed))

        sorted_indices = sorted(
            (
                (index_name, index_stats['index']['primary_size_in_bytes'])
                for (index_name, index_stats) in stats['indices'].items()
            ),
            reverse=True
        )

        for index_name, index_size in sorted_indices:
            disk_usage += index_size

            if disk_usage > disk_limit:
                delete_list.append(index_name)
            else:
                logger.info('skipping {0}, summed disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))
        if delete_list:
            return delete_indices(client, delete_list)
    else:
        logger.warn('No indices to delete_by_space.')
        return True

def delete(client, indices, disk_space=None):
    """
    Helper method called by the script.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg disk_space: Delete indices over *n* gigabytes, alphabetically sorted.
    """

    if disk_space:
        return delete_by_space(client, indices, disk_space=disk_space)
    else:
        return delete_indices(client, indices)
