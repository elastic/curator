from .utils import *

def delete_indices(client, indices):
    """
    Delete the indicated indices, including closed indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    """
    indices = ensure_list(indices)
    try:
        client.indices.delete(index=to_csv(indices))
        return True
    except:
        logger.error("Error deleting indices.  Check logs for more information.")
        return False
