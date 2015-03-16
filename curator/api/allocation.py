from .utils import *
import elasticsearch
import logging
logger = logging.getLogger(__name__)

def apply_allocation_rule(client, indices, rule=None):
    """
    Apply a required allocation rule to a list of indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg rule: The routing allocation rule to apply, e.g. ``tag=ssd``.  Must be
        in the format of ``key=value``, and should match values declared on the
        correlating nodes in your cluster.
    :rtype: bool

    .. note::
        See:
        http://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-allocation.html#index-modules-allocation
    """
    if not rule:
        logger.error('Missing rule parameter')
        return False
    key = rule.split('=')[0]
    value = rule.split('=')[1]
    indices = prune_allocated(client, indices, key, value)
    if not indices:
        logger.warn("No indices to act on.")
        return False
    logger.info('Updating index setting index.routing.allocation.require.{0}={1}'.format(key,value))
    try:
        client.indices.put_settings(index=to_csv(indices),
            body='index.routing.allocation.require.{0}={1}'.format(key,value),
            )
        return True
    except:
        logger.error("Error in updating index settings with allocation rule.  Check logs for more information.")
        return False

def allocation(client, indices, rule=None):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg rule: The routing allocation rule to apply, e.g. ``tag=ssd``.  Must be
        in the format of ``key=value``, and should match values declared on the
        correlating nodes in your cluster.
    :rtype: bool
    """
    return apply_allocation_rule(client, indices, rule=rule)
