from .utils import *

import logging
logger = logging.getLogger(__name__)

def disable_bloom_filter(client, indices):
    """
    Disable the bloom filter cache for the list of indices.
    This method will ignore unavailable (including closed) indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    """
    no_more_bloom = (1, 4, 0)
    version_number = get_version(client)
    indices = ensure_list(indices)
    if version_number >= no_more_bloom:
        logger.warn('Bloom filters no longer exist for search in Elasticsearch since v1.4.0')
    else:
        try:
            client.indices.put_settings(index=to_csv(indices),
                body='index.codec.bloom.load=false', ignore_unavailable=True)
            return True
        except:
            logger.error("Error disabling bloom filters.  Check logs for more information.")
            return False
