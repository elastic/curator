"""Helper utilities

The kind that don't fit in testers, getters, date_ops, or converters
"""
import logging
from es_client.helpers.utils import ensure_list
from curator.exceptions import FailedExecution

def chunk_index_list(indices):
    """
    This utility chunks very large index lists into 3KB chunks.
    It measures the size as a csv string, then converts back into a list for the return value.

    :param indices: The list of indices

    :type indices: list

    :returns: A list of lists (each a piece of the original ``indices``)
    :rtype: list
    """
    chunks = []
    chunk = ""
    for index in indices:
        if len(chunk) < 3072:
            if not chunk:
                chunk = index
            else:
                chunk += "," + index
        else:
            chunks.append(chunk.split(','))
            chunk = index
    chunks.append(chunk.split(','))
    return chunks

def report_failure(exception):
    """
    Raise a :py:exc:`~.curator.exceptions.FailedExecution` exception and include the original error
    message.

    :param exception: The upstream exception.

    :type exception: :py:exc:Exception

    :rtype: None
    """
    raise FailedExecution(
        f'Exception encountered.  Rerun with loglevel DEBUG and/or check Elasticsearch logs for'
        f'more information. Exception: {exception}'
    )

def show_dry_run(ilo, action, **kwargs):
    """
    Log dry run output with the action which would have been executed.

    :param ilo: An IndexList Object
    :param action: The ``action`` to be performed.
    :param kwargs: Any other args to show in the log output


    :type ilo: :py:class:`~.curator.indexlist.IndexList`
    :type action: str
    :type kwargs: dict

    :rtype: None
    """
    logger = logging.getLogger(__name__)
    logger.info('DRY-RUN MODE.  No changes will be made.')
    msg = f'(CLOSED) indices may be shown that may not be acted on by action "{action}".'
    logger.info(msg)
    indices = sorted(ilo.indices)
    for idx in indices:
        # Dry runs need index state, so we collect it here if it's not present.
        try:
            index_closed = ilo.index_info[idx]['state'] == 'close'
        except KeyError:
            ilo.get_index_state()
            index_closed = ilo.index_info[idx]['state'] == 'close'
        var = ' (CLOSED)' if index_closed else ''
        msg = f'DRY-RUN: {action}: {idx}{var} with arguments: {kwargs}'
        logger.info(msg)

def to_csv(indices):
    """
    :param indices: A list of indices to act on, or a single value, which could be
        in the format of a csv string already.

    :type indices: list

    :returns: A csv string from a list of indices, or a single value if only one value is present
    :rtype: str
    """
    indices = ensure_list(indices) # in case of a single value passed
    if indices:
        return ','.join(sorted(indices))
    return None
