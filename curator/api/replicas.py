from .utils import *
import elasticsearch
import logging
logger = logging.getLogger(__name__)

def change_replicas(client, indices, replicas=None):
    """
    Change the number of replicas, more or less, for the indicated indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg replicas: The number of replicas the indices should have
    :rtype: bool
    """
    if replicas == None:
        logger.error('No replica count provided.')
        return False
    else:
        indices = ensure_list(indices)
        logger.info('Updating index setting: number_of_replicas={0}'.format(replicas))
        try:
            client.indices.put_settings(index=to_csv(indices),
                body='number_of_replicas={0}'.format(replicas))
            return True
        except Exception:
            logger.error("Error changing replica count.  Check logs for more information.")
            return False

def replicas(client, indices, replicas=None):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg replicas: The number of replicas the indices should have
    :rtype: bool
    """
    return change_replicas(client, indices, replicas=replicas)
