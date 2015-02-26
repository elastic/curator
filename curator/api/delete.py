from .utils import *
from .filter import *
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
    except Exception as e:
        logger.error("Error deleting indices.  Exception {0}  Check logs for more information.".format(e.message))
        return False

def delete(client, indices, disk_space=None, reverse=True):
    """
    Helper method called by the script.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg disk_space: Delete indices over *n* gigabytes, alphabetically sorted.
    """

    if disk_space:
        indices = filter_by_space(client, indices, disk_space=disk_space, reverse=reverse)
    return delete_indices(client, indices)
