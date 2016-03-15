from .utils import *
import elasticsearch
import logging
logger = logging.getLogger(__name__)

def seal_indices(client, indices):
    """
    Seal the indicated indices (perform a synced flush) if your Elasticsearch
    version supports it before closing.
    This method will ignore unavailable (including closed) indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: bool
    """
    indices = prune_closed(client, indices)
    es_version = get_version(client)
    errcode = ''
    results = {}
    try:
        if es_version >= (1, 6, 0):
            if len(indices) > 0:
                results = client.indices.flush_synced(index=to_csv(indices))
            else:
                logger.warn('No indices to seal.')
                return True
        else:
            logger.error('Your version of Elasticsearch ({0}) does not support index sealing (synced flush).  Must be 1.6.0 or higher.'.format(es_version))
    except Exception as e:
        logger.warn('Non-fatal error encountered.')
        try:
            logger.debug('Error: {0}.  Message: {1}'.format(e.status_code, e.error))
            results = e.info
        except AttributeError:
            logger.debug('Error: {0}'.format(e))
    total = results.pop('_shards') if '_shards' in results else {"total":0,"successful":0,"failed":0}
    fails = [ i for i in sorted(results) if results[i]['failed'] > 0 ]
    if len(fails) > 0:
        logger.warn('{0} indices failed to seal (synced flush):'.format(len(fails)))
        for f in fails:
            reasons = set()
            for shard in results[f]['failures']:
                reasons.add(shard['reason'])
            logger.error('{0}: {1}'.format(f, reasons))
            results.pop(f) # Remove fails from results
        if len(results) > 0:
            logger.info('All other indices provided have been successfully sealed. (Shown with --debug flag enabled.)')
    else:
        logger.info('Provided indices successfully sealed. (Shown with --debug flag enabled.)')
    if len(results) > 0:
        logger.debug('Successfully sealed indices: {0}'.format(list(results.keys())))
    return True # Failures are sometimes expected.  Log viewing is essential to ascertaining failures.

def seal(client, indices):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :rtype: bool
    """

    return seal_indices(client, indices)
