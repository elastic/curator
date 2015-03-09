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
    except Exception:
        logger.error("Error deleting indices.  Check logs for more information.")
        return False

def delete(client, indices):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    """
    return delete_indices(client, indices)
