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
    if not max_num_segments:
        logger.error("Mising value for max_num_segments.")
        return False
    if check_csv(index_name):
        logger.error("Must specify only a single index as an argument.")
        return False
    if index_closed(client, index_name): # Don't try to optimize a closed index
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        shards, segmentcount = get_segmentcount(client, index_name)
        logger.debug('Index {0} has {1} shards and {2} segments total.'.format(index_name, shards, segmentcount))
        if segmentcount > (shards * max_num_segments):
            logger.info('Optimizing index {0} to {1} segments per shard.  Please wait...'.format(index_name, max_num_segments))
            try:
                client.indices.optimize(index=index_name, max_num_segments=max_num_segments, request_timeout=request_timeout)
                return True
            except Exception:
                logger.error("Error optimizing index {0}.  Check logs for more information.".format(index_name))
                return False
        else:
            logger.info('Skipping index {0}: Already optimized.'.format(index_name))
            return True

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
        # If we fail once, we fail completely
        success = optimize_index(client, i, max_num_segments=max_num_segments, request_timeout=request_timeout)
        if not success:
            retval = False
        time.sleep(delay)
    return retval
