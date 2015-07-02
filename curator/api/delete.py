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
        logger.info("Deleting indices as a batch operation:")
        for i in indices:
            logger.info("---deleting index {0}".format(i))
        client.indices.delete(index=to_csv(indices))
        return True
    except Exception:
        logger.error("Error deleting one or more indices.  Run with --debug flag and/or check Elasticsearch logs for more information.")
        return False

def delete(client, indices):
    """
    Helper method called by the CLI.  Tries up to 3x to delete indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    """
    for count in range(1, 4): # Try 3 times
        success = delete_indices(client, indices)
        if success:
            result = [ i for i in indices if i in get_indices(client) ]
            if len(result) > 0:
                logger.error("Indices failed to delete:")
                for idx in result:
                    logger.error("---{0}".format(idx))
            else:
                # break
                return True # We leave the loop here, if everything deleted.
            indices = result
        else:
            return False # Encountered an exception
    logger.error("Unable to delete indices after 3 attempts: {0}".format(result))
    return False # If we make it here, indices didn't delete after 3 tries.
