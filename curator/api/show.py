from .utils import *

def show(client, object_list, type=None):
    """
    Helper method called by the CLI.

    :arg client: The Elasticsearch client connection
    :arg object_list: A list of indices or snapshots to show
    :arg object_type: `indices` or `snapshots`
    :rtype: bool
    """
    for obj in ensure_list(object_list):
        if type == 'indices':
            print('{0}{1}'.format(obj, ' (CLOSED)' if index_closed(client, obj) else ''))
        else:
            print('{0}'.format(obj))
    return True
