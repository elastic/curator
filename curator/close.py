def close_indices(client, indices):
    """
    Close the indicated indices.  Flush before closing.
    This method will ignore unavailable (including closed) indices.

    :arg client: The Elasticsearch client connection
    :arg indices: A list of indices to act on
    """
    indices = ensure_list(indices)
    try:
        client.indices.flush(index=to_csv(indices), ignore_unavailable=True)
        client.indices.close(index=to_csv(indices), ignore_unavailable=True)
        return True
    except:
        logger.error("Error closing indices.  Check logs for more information.")
        return False
