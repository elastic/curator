### Allocation
def apply_allocation_rule(client, indices, rule=None):
    """
    Apply a required allocation rule to a list of indices.  See:
    http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index-modules-allocation.html#index-modules-allocation
    This method will ignore unavailable (including closed) indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    :arg rule: The routing allocation rule to apply, e.g. ``tag=ssd``.  Must be
        in the format of ``key=value``, and should match values declared on the
        correlating nodes in your cluster.
    """
    if not rule:
        logger.error('No rule provided for {0}.'.format(index_name))
        return False
    key = rule.split('=')[0]
    value = rule.split('=')[1]
    indices = ensure_list(indices)
    logger.info('Updating index setting index.routing.allocation.require.{0}={1}'.format(key,value))
    try:
        client.indices.put_settings(index=to_csv(indices),
            body='index.routing.allocation.require.{0}={1}'.format(key,value),
            ignore_unavailable=True)
        return True
    except:
        logger.error("Error in updating index settings with allocation rule.  Check logs for more information.")
        return False
