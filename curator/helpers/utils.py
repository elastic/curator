"""Helper utilities

The kind that don't fit in testers, getters, date_ops, or converters
"""

import re
import logging
from es_client.helpers.utils import ensure_list
from curator.exceptions import FailedExecution

logger = logging.getLogger(__name__)


def chunk_index_list(indices):
    """
    This utility chunks very large index lists into 3KB chunks.
    It measures the size as a csv string, then converts back into a list for the
    return value.

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
    Raise a :py:exc:`~.curator.exceptions.FailedExecution` exception and include
    the original error message.

    :param exception: The upstream exception.

    :type exception: :py:exc:Exception

    :rtype: None
    """
    raise FailedExecution(
        f'Exception encountered.  Rerun with loglevel DEBUG and/or check '
        f'Elasticsearch logs for more information. Exception: {exception}'
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
    logger.info('DRY-RUN MODE.  No changes will be made.')
    msg = (
        f'(CLOSED) indices may be shown that may not be acted on by action "{action}".'
    )
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

    :returns: A csv string from a list of indices, or a single value if only one
        value is present
    :rtype: str
    """
    indices = ensure_list(indices)  # in case of a single value passed
    if indices:
        return ','.join(sorted(indices))
    return None


def multitarget_fix(pattern: str) -> str:
    """
    If pattern only has '-' prefixed entries (excludes)
    prepend a wildcard to pattern

    :param pattern: The Elasticsearch multi-target syntax pattern
    :type pattern: str

    :returns: The pattern, possibly with a wildcard prepended
    :rtype: str
    """
    # Split pattern into elements
    elements = pattern.split(',')
    if len(elements) == 1 and elements[0] == '':
        return '*'
    # Initialize positive and negative lists
    positives = []
    negatives = []
    # Loop through elements and sort them into positive and negative lists
    for element in elements:
        if element.startswith('-'):
            negatives.append(element)
        else:
            positives.append(element)
    # If there are no positive elements, but there are negative elements,
    # add a wildcard to the beginning of the pattern
    if len(positives) == 0 and len(negatives) > 0:
        logger.debug(
            "Only negative elements in pattern. Adding '*' so there's something "
            " to remove"
        )
        return '*,' + pattern
    # Otherwise, return the original pattern
    return pattern


def regex_loop(matchstr: str, indices: list) -> list:
    """
    Loop through indices,
    Match against matchstr,
    return matches

    :param matchstr: The Python regex pattern to match against
    :param indices: The list of indices to match against
    :type matchstr: str
    :type indices: list
    :returns: The list of matching indices
    :rtype: list
    """
    retval = []
    for idx in indices:
        if re.match(matchstr, idx):
            retval.append(idx)
    return retval


def multitarget_match(pattern: str, index_list: list) -> list:
    """
    Convert Elasticsearch multi-target syntax ``pattern`` into Python regex
    patterns. Match against ``index_list`` and return the list of matches while
    excluding any negative matches.

    :param pattern: The Elasticsearch multi-target syntax pattern
    :param index_list: The list of indices to match against
    :type pattern: str
    :type index_list: list

    :returns: The final resulting list of indices
    :rtype: list
    """
    retval = []
    excluded = []
    includes = []
    excludes = []
    logger.debug('Multi-target syntax pattern: %s', pattern)
    elements = multitarget_fix(pattern).split(',')
    logger.debug('Individual elements of pattern: %s', elements)
    for element in elements:
        # Exclude elements are prefixed with '-'
        exclude = element.startswith('-')
        # Any index prepended with a . is probably a hidden index, but
        # we need to escape the . for regex to treat it as a literal
        matchstr = element.replace('.', '\\.')
        # Replace Elasticsearch wildcard * with .* for Python regex
        matchstr = matchstr.replace('*', '.*')
        # logger.debug('matchstr: %s', matchstr)
        # If it is not an exclude, add the output of regex_loop to `includes`
        if not exclude:
            includes += regex_loop(matchstr, index_list)
        # If it is an exclude pattern, add the output of regex_loop to `excludes`
        # Remove the `-` from the matchstr ([1:]) before sending.
        if exclude:
            excludes += regex_loop(matchstr[1:], index_list)
    # Create a unique list of indices to loop through
    for idx in list(set(includes)):
        # If idx is not in the unique list of excludes, add it to retval
        if idx not in list(set(excludes)):
            retval.append(idx)
        else:
            # Otherwise, add it to the excludes
            excluded.append(idx)
    # Sort the lists alphabetically
    retval.sort()
    excluded.sort()
    # Log the results
    logger.debug('Included indices: %s', retval)
    logger.debug('Excluded indices: %s', excluded)
    return retval
