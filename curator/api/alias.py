from .utils import *
import elasticsearch
import logging
logger = logging.getLogger(__name__)

def add_to_alias(client, index_name, alias=None):
    """
    Add indicated index to the specified alias.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg alias: Alias name to operate on.
    :rtype: bool
    """
    if check_csv(index_name):
        logger.error("Must specify only a single index as an argument.")
        return False
    if not alias: # This prevents _all from being aliased by accident...
        logger.error('No alias provided.')
        return False
    if not client.indices.exists_alias(alias):
        logger.error('Skipping index {0}: Alias {1} does not exist.'.format(index_name, alias))
        return False
    else:
        indices_in_alias = get_alias(client, alias)
        if not index_name in indices_in_alias:
            if index_closed(client, index_name):
                logger.error('Failed to add index {0} to alias {1} because it is closed.'.format(index_name, alias))
                return False
            else:
                try:
                    client.indices.update_aliases(body={'actions': [{ 'add': { 'index': index_name, 'alias': alias}}]})
                    return True
                except Exception as e:
                    logger.error("Error adding index {0} to alias {1}.  Exception: {2}  Check logs for more information.".format(index_name, alias, e))
                    return False
        else:
            logger.info('Skipping index {0}: Index already exists in alias {1}...'.format(index_name, alias))
            return True

def remove_from_alias(client, index_name, alias=None):
    """
    Remove the indicated index from the specified alias.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg alias: Alias name to operate on.
    :rtype: bool
    """
    if check_csv(index_name):
        logger.error("Must specify only a single index as an argument.")
        return False
    if not alias:
        logger.error('No alias provided.')
        return False
    indices_in_alias = get_alias(client, alias)
    if not indices_in_alias:
        logger.error("Index {0} not found in alias {1}".format(index_name, alias))
        return False
    if index_name in indices_in_alias:
        try:
            client.indices.update_aliases(body={'actions': [{ 'remove': { 'index': index_name, 'alias': alias}}]})
            return True
        except Exception as e:
            logger.error("Error removing index {0} from alias {1}.  Exception: {2}  Check logs for more information.".format(index_name, alias, e))
            return False
    else:
        logger.warn('Index {0} does not exist in alias {1}; skipping.'.format(index_name, alias))
        return False

def alias(client, indices, alias=None, remove=False):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg alias: Alias name to operate on.
    :arg remove: If true, remove the alias.
    :rtype: bool
    """
    retval = True
    for i in ensure_list(indices):
        if remove:
            success = remove_from_alias(client, i, alias=alias)
        else:
            success = add_to_alias(client, i, alias=alias)
        # if we fail once, we fail completely
        if not success:
            retval = False
    return retval
