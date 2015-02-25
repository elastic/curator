from .utils import *

import logging
logger = logging.getLogger(__name__)

def get_alias(client, alias):
    """
    Return information about the specified alias.

    :arg client: The Elasticsearch client connection
    :arg alias: Alias name to operate on.
    :rtype: list of strings
    """
    if client.indices.exists_alias(alias):
        return client.indices.get_alias(name=alias).keys()
    else:
        logger.error('Unable to find alias {0}.'.format(alias))
        return False

def add_to_alias(client, index_name, alias=None):
    """
    Add indicated index to the specified alias.

    :arg client: The Elasticsearch client connection
    :arg index_name: The index name
    :arg alias: Alias name to operate on.
    :rtype: bool
    """
    if csv_check(index_name):
        logger.error("Must specify only a single index as an argument.")
        return True
    if not alias: # This prevents _all from being aliased by accident...
        logger.error('No alias provided.')
        return True
    if not client.indices.exists_alias(alias):
        logger.error('Skipping index {0}: Alias {1} does not exist.'.format(index_name, alias))
        return True
    else:
        indices_in_alias = client.indices.get_alias(alias)
        if not index_name in indices_in_alias:
            if index_closed(client, index_name):
                logger.info('Skipping index {0}: Already closed.'.format(index_name))
                return True
            else:
                client.indices.update_aliases(body={'actions': [{ 'add': { 'index': index_name, 'alias': alias}}]})
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
    if csv_check(index_name):
        logger.error("Must specify only a single index as an argument.")
        return True
    indices_in_alias = get_alias(client, alias)
    if not indices_in_alias:
        return True
    if index_name in indices_in_alias:
        client.indices.update_aliases(body={'actions': [{ 'remove': { 'index': index_name, 'alias': alias}}]})
    else:
        logger.info('Index {0} does not exist in alias {1}; skipping.'.format(index_name, alias))
        return True
