from .utils import *
import elasticsearch
import logging
logger = logging.getLogger(__name__)

def change_replicas(client, indices, replicas=None):
    """
    Change the number of replicas, more or less, for the indicated indices.
    This method will ignore closed indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg replicas: The number of replicas the indices should have
    :rtype: bool
    """
    if replicas == None:
        logger.error('No replica count provided.')
        return False
    else:
        indices = prune_closed(client, indices)
        logger.info('Updating index setting: number_of_replicas={0}'.format(replicas))
        try:
            client.indices.put_settings(index=to_csv(indices),
                body='number_of_replicas={0}'.format(replicas))
            return True
        except Exception as e:
            logger.error("Error changing replica count.  Exception {0}  Check logs for more information.".format(e.message))
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
