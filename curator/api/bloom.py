from .utils import *
import elasticsearch
import time
import logging
logger = logging.getLogger(__name__)

def disable_bloom_filter(client, indices, delay=None):
    """
    Disable the bloom filter cache for the list of indices.
    This method will ignore closed indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg delay: Pause *n* seconds after operating on each index
    :rtype: bool
    """
    indices = prune_closed(client, indices)
    no_more_bloom = (1, 4, 0)
    version_number = get_version(client)
    if version_number >= no_more_bloom:
        logger.warn('Bloom filters no longer exist for search in Elasticsearch since v1.4.0')
        return True
    else:
        try:
            if delay:
                if delay > 0:
                    return loop_bloom(client, indices, delay)
            else:
                client.indices.put_settings(index=to_csv(indices),
                    body='index.codec.bloom.load=false')
                return True
        except Exception:
            logger.error("Error disabling bloom filters.  Check logs for more information.")
            return False

def loop_bloom(client, indices, delay):
    """
    Iterate over list of indices.  Only called from within
    :py:func:`curator.api.disable_bloom_filter`

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg delay: Pause *n* seconds after operating on each index
    :rtype: bool
    """
    retval = True
    for i in indices:
        success = disable_bloom_filter(client, i)
        # If fail on even one iteration, we fail period
        if not success:
            retval = False
        time.sleep(delay)
    return retval

def bloom(client, indices, delay=None):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg delay: Pause *n* seconds after operating on each index
    :rtype: bool
    """

    return disable_bloom_filter(client, indices, delay=delay)
