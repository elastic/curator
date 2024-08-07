"""Utility functions that get things"""

import logging
from voluptuous import Schema
from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import NotFoundError
from es_client.helpers.schemacheck import SchemaCheck
from es_client.helpers.utils import prune_nones
from curator.helpers.getters import get_repository, get_write_index
from curator.exceptions import (
    ConfigurationError,
    MissingArgument,
    RepositoryException,
    SearchableSnapshotException,
)
from curator.defaults.settings import (
    index_filtertypes,
    snapshot_actions,
    snapshot_filtertypes,
)
from curator.validators import actions, options
from curator.validators.filter_functions import validfilters
from curator.helpers.utils import report_failure


def has_lifecycle_name(idx_settings):
    """
    :param idx_settings: The settings for an index being tested
    :type idx_settings: dict

    :returns: ``True`` if a lifecycle name exists in settings, else ``False``
    :rtype: bool
    """
    if 'lifecycle' in idx_settings:
        if 'name' in idx_settings['lifecycle']:
            return True
    return False


def is_idx_partial(idx_settings):
    """
    :param idx_settings: The settings for an index being tested
    :type idx_settings: dict

    :returns: ``True`` if store.snapshot.partial exists in settings, else ``False``
    :rtype: bool
    """
    if 'store' in idx_settings:
        if 'snapshot' in idx_settings['store']:
            if 'partial' in idx_settings['store']['snapshot']:
                if idx_settings['store']['snapshot']['partial']:
                    return True
                # store.snapshot.partial exists but is False -- Not a frozen tier mount
                return False
            # store.snapshot exists, but partial isn't there --
            # Possibly a cold tier mount
            return False
        raise SearchableSnapshotException('Index not a mounted searchable snapshot')
    raise SearchableSnapshotException('Index not a mounted searchable snapshot')


def ilm_policy_check(client, alias):
    """Test if alias is associated with an ILM policy

    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_settings`

    :param client: A client connection object
    :param alias: The alias name

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type alias: str
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    # alias = action_obj.options['name']
    write_index = get_write_index(client, alias)
    try:
        idx_settings = client.indices.get_settings(index=write_index)
        if 'name' in idx_settings[write_index]['settings']['index']['lifecycle']:
            # logger.info('Alias %s is associated with ILM policy.', alias)
            # logger.info('Skipping action %s because allow_ilm_indices is false.', idx)
            return True
    except KeyError:
        logger.debug('No ILM policies associated with %s', alias)
    return False


def repository_exists(client, repository=None):
    """
    Calls :py:meth:`~.elasticsearch.client.SnapshotClient.get_repository`

    :param client: A client connection object
    :param repository: The Elasticsearch snapshot repository to use

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type repository: str

    :returns: ``True`` if ``repository`` exists, else ``False``
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    if not repository:
        raise MissingArgument('No value for "repository" provided')
    try:
        test_result = get_repository(client, repository)
        if repository in test_result:
            logger.debug("Repository %s exists.", repository)
            response = True
        else:
            logger.debug("Repository %s not found...", repository)
            response = False
    # pylint: disable=broad-except
    except Exception as err:
        logger.debug('Unable to find repository "%s": Error: %s', repository, err)
        response = False
    return response


def rollable_alias(client, alias):
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_alias`

    :param client: A client connection object
    :param alias: An Elasticsearch alias

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type alias: str


    :returns: ``True`` or ``False`` depending on whether ``alias`` is an alias that
        points to an index that can be used by the ``_rollover`` API.
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    try:
        response = client.indices.get_alias(name=alias)
    except NotFoundError:
        logger.error('Alias "%s" not found.', alias)
        return False
    # Response should be like:
    # {'there_should_be_only_one': {'aliases': {'value of "alias" here': {}}}}
    # where 'there_should_be_only_one' is a single index name that ends in a number,
    # and 'value of "alias" here' reflects the value of the passed parameter, except
    # where the ``is_write_index`` setting makes it possible to have more than one
    # index associated with a rollover index
    for idx in response:
        if 'is_write_index' in response[idx]['aliases'][alias]:
            if response[idx]['aliases'][alias]['is_write_index']:
                return True
    # implied ``else``: If not ``is_write_index``, it has to fit the following criteria:
    if len(response) > 1:
        logger.error(
            '"alias" must only reference one index, but points to %s', response
        )
        return False
    index = list(response.keys())[0]
    rollable = False
    # In order for `rollable` to be True, the last 2 digits of the index
    # must be digits, or a hyphen followed by a digit.
    # NOTE: This is not a guarantee that the rest of the index name is
    # necessarily correctly formatted.
    if index[-2:][1].isdigit():
        if index[-2:][0].isdigit():
            rollable = True
        elif index[-2:][0] == '-':
            rollable = True
    return rollable


def snapshot_running(client):
    """
    Calls :py:meth:`~.elasticsearch.client.SnapshotClient.get_repository`

    Return ``True`` if a snapshot is in progress, and ``False`` if not

    :param client: A client connection object

    :type client: :py:class:`~.elasticsearch.Elasticsearch`

    :rtype: bool
    """
    try:
        status = client.snapshot.status()['snapshots']
    # pylint: disable=broad-except
    except Exception as exc:
        report_failure(exc)
    # We will only accept a positively identified False.  Anything else is
    # suspect. That's why this statement, rather than just ``return status``
    # pylint: disable=simplifiable-if-expression
    return False if not status else True


def validate_actions(data):
    """
    Validate the ``actions`` configuration dictionary, as imported from actions.yml,
    for example.

    :param data: The configuration dictionary

    :type data: dict

    :returns: The validated and sanitized configuration dictionary.
    :rtype: dict
    """
    # data is the ENTIRE schema...
    clean_config = {}
    # Let's break it down into smaller chunks...
    # First, let's make sure it has "actions" as a key, with a subdictionary
    root = SchemaCheck(data, actions.root(), 'Actions File', 'root').result()
    # We've passed the first step.  Now let's iterate over the actions...
    for action_id in root['actions']:
        # Now, let's ensure that the basic action structure is correct, with
        # the proper possibilities for 'action'
        action_dict = root['actions'][action_id]
        loc = f'Action ID "{action_id}"'
        valid_structure = SchemaCheck(
            action_dict, actions.structure(action_dict, loc), 'structure', loc
        ).result()
        # With the basic structure validated, now we extract the action name
        current_action = valid_structure['action']
        # And let's update the location with the action.
        loc = f'Action ID "{action_id}", action "{current_action}"'
        clean_options = SchemaCheck(
            prune_nones(valid_structure['options']),
            options.get_schema(current_action),
            'options',
            loc,
        ).result()
        clean_config[action_id] = {
            'action': current_action,
            'description': valid_structure['description'],
            'options': clean_options,
        }
        if current_action == 'alias':
            add_remove = {}
            for k in ['add', 'remove']:
                if k in valid_structure:
                    current_filters = SchemaCheck(
                        valid_structure[k]['filters'],
                        Schema(validfilters(current_action, location=loc)),
                        f'"{k}" filters',
                        f'{loc}, "filters"',
                    ).result()
                    add_remove.update(
                        {
                            k: {
                                'filters': SchemaCheck(
                                    current_filters,
                                    Schema(validfilters(current_action, location=loc)),
                                    'filters',
                                    f'{loc}, "{k}", "filters"',
                                ).result()
                            }
                        }
                    )
            # Add/Remove here
            clean_config[action_id].update(add_remove)
        elif current_action in ['cluster_routing', 'create_index', 'rollover']:
            # neither cluster_routing nor create_index should have filters
            pass
        else:  # Filters key only appears in non-alias actions
            valid_filters = SchemaCheck(
                valid_structure['filters'],
                Schema(validfilters(current_action, location=loc)),
                'filters',
                f'{loc}, "filters"',
            ).result()
            clean_filters = validate_filters(current_action, valid_filters)
            clean_config[action_id].update({'filters': clean_filters})
        # This is a special case for remote reindex
        if current_action == 'reindex':
            # Check only if populated with something.
            if 'remote_filters' in valid_structure['options']:
                valid_filters = SchemaCheck(
                    valid_structure['options']['remote_filters'],
                    Schema(validfilters(current_action, location=loc)),
                    'filters',
                    f'{loc}, "filters"',
                ).result()
                clean_remote_filters = validate_filters(current_action, valid_filters)
                clean_config[action_id]['options'].update(
                    {'remote_filters': clean_remote_filters}
                )

    # if we've gotten this far without any Exceptions raised, it's valid!
    return {'actions': clean_config}


def validate_filters(action, myfilters):
    """
    Validate that myfilters are appropriate for the action type, e.g. no
    index filters applied to a snapshot list.

    :param action: An action name
    :param myfilters: A list of filters to test.

    :type action: str
    :type myfilters: list

    :returns: Validated list of filters
    :rtype: list
    """
    # Define which set of filtertypes to use for testing
    if action in snapshot_actions():
        filtertypes = snapshot_filtertypes()
    else:
        filtertypes = index_filtertypes()
    for fil in myfilters:
        if fil['filtertype'] not in filtertypes:
            raise ConfigurationError(
                f"\"{fil['filtertype']}\" filtertype is not compatible with "
                f"action \"{action}\""
            )
    # If we get to this point, we're still valid.  Return the original list
    return myfilters


def verify_client_object(test):
    """
    :param test: The variable or object to test

    :type test: :py:class:`~.elasticsearch.Elasticsearch`

    :returns: ``True`` if ``test`` is a proper :py:class:`~.elasticsearch.Elasticsearch`
        client object and raise a :py:exc:`TypeError` exception if it is not.
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    # Ignore mock type for testing
    if str(type(test)) == "<class 'unittest.mock.Mock'>":
        pass
    elif not isinstance(test, Elasticsearch):
        msg = f'Not a valid client object. Type: {type(test)} was passed'
        logger.error(msg)
        raise TypeError(msg)


def verify_index_list(test):
    """
    :param test: The variable or object to test

    :type test: :py:class:`~.curator.IndexList`

    :returns: ``None`` if ``test`` is a proper :py:class:`~.curator.indexlist.IndexList`
        object, else raise a :py:class:`TypeError` exception.
    :rtype: None
    """
    # It breaks if this import isn't local to this function:
    # ImportError: cannot import name 'IndexList' from partially initialized module
    # 'curator.indexlist' (most likely due to a circular import)
    # pylint: disable=import-outside-toplevel
    from curator.indexlist import IndexList

    logger = logging.getLogger(__name__)
    if not isinstance(test, IndexList):
        msg = f'Not a valid IndexList object. Type: {type(test)} was passed'
        logger.error(msg)
        raise TypeError(msg)


def verify_repository(client, repository=None):
    """
    Do :py:meth:`~.elasticsearch.snapshot.verify_repository` call. If it fails, raise a
    :py:exc:`~.curator.exceptions.RepositoryException`.

    :param client: A client connection object
    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :param repository: A repository name

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type repository: str

    :rtype: None
    """
    logger = logging.getLogger(__name__)
    try:
        nodes = client.snapshot.verify_repository(name=repository)['nodes']
        logger.debug('All nodes can write to the repository')
        logger.debug('Nodes with verified repository access: %s', nodes)
    except Exception as err:
        try:
            if err.status_code == 404:
                msg = (
                    f'--- Repository "{repository}" not found. Error: '
                    f'{err.meta.status}, {err.error}'
                )
            else:
                msg = (
                    f'--- Got a {err.meta.status} response from Elasticsearch.  '
                    f'Error message: {err.error}'
                )
        except AttributeError:
            msg = f'--- Error message: {err}'.format()
        report = f'Failed to verify all nodes have repository access: {msg}'
        raise RepositoryException(report) from err


def verify_snapshot_list(test):
    """
    :param test: The variable or object to test

    :type test: :py:class:`~.curator.SnapshotList`

    :returns: ``None`` if ``test`` is a proper
        :py:class:`~.curator.snapshotlist.SnapshotList` object, else raise a
        :py:class:`TypeError` exception.
    :rtype: None
    """
    # It breaks if this import isn't local to this function:
    # ImportError: cannot import name 'SnapshotList' from partially initialized module
    # 'curator.snapshotlist' (most likely due to a circular import)
    # pylint: disable=import-outside-toplevel
    from curator.snapshotlist import SnapshotList

    logger = logging.getLogger(__name__)
    if not isinstance(test, SnapshotList):
        msg = f'Not a valid SnapshotList object. Type: {type(test)} was passed'
        logger.error(msg)
        raise TypeError(msg)
