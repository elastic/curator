from .utils import *
import elasticsearch

import logging
logger = logging.getLogger(__name__)

def show(object_list):
    """
    Helper method called by the script.

    :arg client: The Elasticsearch client connection
    :arg object_list: A list of indices or snapshots to show
    :rtype: bool
    """
    for obj in ensure_list(object_list):
        print('{0}'.format(obj))
    return True
