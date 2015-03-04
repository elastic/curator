from .utils import *
import elasticsearch
import logging
logger = logging.getLogger(__name__)

def close_indices(client, indices):
    """
    Close the indicated indices.  Flush before closing.
    This method will ignore unavailable (including closed) indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: bool
    """
    indices = ensure_list(indices)
    try:
        client.indices.flush(index=to_csv(indices), ignore_unavailable=True)
        client.indices.close(index=to_csv(indices), ignore_unavailable=True)
        return True
    except Exception:
        logger.error("Error closing indices.  Check logs for more information.")
        return False

def close(client, indices):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: bool
    """

    return close_indices(client, indices)
