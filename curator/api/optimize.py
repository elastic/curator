from .utils import *
import elasticsearch
import time
import logging
logger = logging.getLogger(__name__)

def optimize_index(client, index_name, max_num_segments=None,
                request_timeout=21600):
    """
    Optimize (Lucene forceMerge) index to `max_num_segments` per shard

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg max_num_segments: Merge to this number of segments per shard.
    :rtype: bool
    """
    if optimized(client, index_name, max_num_segments):
        return True
    else:
        logger.info('Optimizing index {0} to {1} segments per shard.  Please wait...'.format(index_name, max_num_segments))
        try:
            client.indices.optimize(index=index_name, max_num_segments=max_num_segments, request_timeout=request_timeout)
            return True
        except Exception:
            logger.error("Error optimizing index {0}.  Check logs for more information.".format(index_name))
            return False

def optimize(client, indices, max_num_segments=None, delay=0, request_timeout=21600):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg max_num_segments: Merge to this number of segments per shard.
    :rtype: bool
    """
    retval = True
    for i in ensure_list(indices):
        wait = True if delay > 0 and not optimized(client, i, max_num_segments) else False
        # If we fail once, we fail completely
        success = optimize_index(client, i, max_num_segments=max_num_segments, request_timeout=request_timeout)
        if not success:
            retval = False
        if wait:
            time.sleep(delay)
    return retval
