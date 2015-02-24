from .utils import *

def change_replicas(client, indices, replicas=None):
    """
    Change the number of replicas, more or less, for the indicated indices.
    This method will ignore unavailable (including closed) indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg replicas: The number of replicas the indices should have
    """
    if replicas == None:
        logger.error('No replica count provided.')
        return False
    else:
        indices = ensure_list(indices)
        logger.info('Updating index setting number_of_replicas={0}'.format(replicas))
        try:
            client.indices.put_settings(index=to_csv(indices),
                body='number_of_replicas={0}'.format(replicas),
                ignore_unavailable=True)
            return True
        except:
            logger.error("Error changing replica count.  Check logs for more information.")
            return False
