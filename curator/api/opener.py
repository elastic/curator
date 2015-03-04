from .utils import *
import elasticsearch
import logging
logger = logging.getLogger(__name__)

def open_indices(client, indices):
    """
    Open the indicated indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: bool
    """
    indices = ensure_list(indices)
    try:
        # Opening an already open index has no effect.
        client.indices.open(index=to_csv(indices))
        return True
    except Exception:
        logger.error("Error opening indices.  Check logs for more information.")
        return False

def opener(client, indices):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: bool
    """
    return open_indices(client, indices)
