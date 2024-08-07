"""Main CLI for Curator"""

import sys
import logging
import click
from es_client.defaults import OPTION_DEFAULTS
from es_client.helpers.config import (
    cli_opts,
    context_settings,
    generate_configdict,
    get_client,
    get_config,
    options_from_dict,
)
from es_client.helpers.logging import configure_logging
from es_client.helpers.utils import option_wrapper, prune_nones
from curator.exceptions import ClientException
from curator.classdef import ActionsFile
from curator.defaults.settings import (
    CLICK_DRYRUN,
    default_config_file,
    footer,
    snapshot_actions,
)
from curator.exceptions import NoIndices, NoSnapshots
from curator.helpers.testers import ilm_policy_check
from curator._version import __version__

ONOFF = {'on': '', 'off': 'no-'}
click_opt_wrap = option_wrapper()

# pylint: disable=R0913, R0914, W0613, W0622, W0718


def ilm_action_skip(client, action_def):
    """
    Skip rollover action if ``allow_ilm_indices`` is ``false``. For all other
    non-snapshot actions, add the ``ilm`` filtertype to the
    :py:attr:`~.curator.ActionDef.filters` list.

    :param action_def: An action object
    :type action_def: :py:class:`~.curator.classdef.ActionDef`

    :returns: ``True`` if ``action_def.action`` is ``rollover`` and the alias
        identified by ``action_def.options['name']`` is associated with an ILM
        policy. This hacky work-around is because the Rollover action does not
        use :py:class:`~.curator.IndexList`
    :rtype: bool
    """
    logger = logging.getLogger(__name__)
    if not action_def.allow_ilm and action_def.action not in snapshot_actions():
        if action_def.action == 'rollover':
            if ilm_policy_check(client, action_def.options['name']):
                logger.info(
                    'Alias %s is associated with ILM policy.',
                    action_def.options['name'],
                )
                return True
        elif action_def.filters:
            action_def.filters.append({'filtertype': 'ilm'})
        else:
            action_def.filters = [{'filtertype': 'ilm'}]
    return False


def exception_handler(action_def, err):
    """Do the grunt work with the exception

    :param action_def: An action object
    :param err: The exception

    :type action_def: :py:class:`~.curator.classdef.ActionDef`
    :type err: :py:exc:`Exception`
    """
    logger = logging.getLogger(__name__)
    if isinstance(err, (NoIndices, NoSnapshots)):
        if action_def.iel:
            logger.info(
                'Skipping action "%s" due to empty list: %s',
                action_def.action,
                type(err),
            )
        else:
            logger.error(
                'Unable to complete action "%s".  No actionable items in list: %s',
                action_def.action,
                type(err),
            )
            sys.exit(1)
    else:
        logger.error(
            'Failed to complete action: %s.  %s: %s', action_def.action, type(err), err
        )
        if action_def.cif:
            logger.info(
                'Continuing execution with next action because "continue_if_exception" '
                'is set to True for action %s',
                action_def.action,
            )
        else:
            sys.exit(1)


def process_action(client, action_def, dry_run=False):
    """
    Do the ``action`` in ``action_def.action``, using the associated options and
    any ``kwargs``.

    :param client: A client connection object
    :param action_def: The ``action`` object

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type action_def: :py:class:`~.curator.classdef.ActionDef`
    :rtype: None
    """
    logger = logging.getLogger(__name__)
    logger.debug('Configuration dictionary: %s', action_def.action_dict)
    mykwargs = {}
    search_pattern = '_all'

    logger.debug('INITIAL Action kwargs: %s', mykwargs)
    # Add some settings to mykwargs...
    if action_def.action == 'delete_indices':
        mykwargs['master_timeout'] = 30

    # Update the defaults with whatever came with opts, minus any Nones
    mykwargs.update(prune_nones(action_def.options))

    # Pop out the search_pattern option, if present.
    if 'search_pattern' in mykwargs:
        search_pattern = mykwargs.pop('search_pattern')

    logger.debug('Action kwargs: %s', mykwargs)
    logger.debug('Post search_pattern Action kwargs: %s', mykwargs)

    # Set up the action
    logger.debug('Running "%s"', action_def.action.upper())
    if action_def.action == 'alias':
        # Special behavior for this action, as it has 2 index lists
        action_def.instantiate('action_cls', **mykwargs)
        action_def.instantiate('alias_adds', client)
        action_def.instantiate('alias_removes', client)
        if 'remove' in action_def.action_dict:
            logger.debug('Removing indices from alias "%s"', action_def.options['name'])
            action_def.alias_removes.iterate_filters(action_def.action_dict['remove'])
            action_def.action_cls.remove(
                action_def.alias_removes,
                warn_if_no_indices=action_def.options['warn_if_no_indices'],
            )
        if 'add' in action_def.action_dict:
            logger.debug('Adding indices to alias "%s"', action_def.options['name'])
            action_def.alias_adds.iterate_filters(action_def.action_dict['add'])
            action_def.action_cls.add(
                action_def.alias_adds,
                warn_if_no_indices=action_def.options['warn_if_no_indices'],
            )
    elif action_def.action in ['cluster_routing', 'create_index', 'rollover']:
        action_def.instantiate('action_cls', client, **mykwargs)
    else:
        if action_def.action in ['delete_snapshots', 'restore']:
            mykwargs.pop('repository')  # We don't need to send this value to the action
            action_def.instantiate(
                'list_obj', client, repository=action_def.options['repository']
            )
        else:
            action_def.instantiate('list_obj', client, search_pattern=search_pattern)
        action_def.list_obj.iterate_filters({'filters': action_def.filters})
        logger.debug('Pre Instantiation Action kwargs: %s', mykwargs)
        action_def.instantiate('action_cls', action_def.list_obj, **mykwargs)
    # Do the action
    if dry_run:
        action_def.action_cls.do_dry_run()
    else:
        logger.debug('Doing the action here.')
        action_def.action_cls.do_action()


def run(ctx: click.Context) -> None:
    """
    :param ctx: The Click command context

    :type ctx: :py:class:`Context <click.Context>`

    Called by :py:func:`cli` to execute what was collected at the command-line
    """
    logger = logging.getLogger(__name__)
    logger.debug('action_file: %s', ctx.params['action_file'])
    all_actions = ActionsFile(ctx.params['action_file'])
    for idx in sorted(list(all_actions.actions.keys())):
        action_def = all_actions.actions[idx]
        # Skip to next action if 'disabled'
        if action_def.disabled:
            logger.info(
                'Action ID: %s: "%s" not performed because "disable_action" '
                'is set to True',
                idx,
                action_def.action,
            )
            continue
        logger.info('Preparing Action ID: %s, "%s"', idx, action_def.action)

        # Override the timeout, if specified, otherwise use the default.
        if action_def.timeout_override:
            ctx.obj['configdict']['elasticsearch']['client'][
                'request_timeout'
            ] = action_def.timeout_override

        # Create a client object for each action...
        logger.info('Creating client object and testing connection')

        try:
            client = get_client(configdict=ctx.obj['configdict'])
        except ClientException as exc:
            # No matter where logging is set to go, make sure we dump these messages to
            # the CLI
            click.echo('Unable to establish client connection to Elasticsearch!')
            click.echo(f'Exception: {exc}')
            sys.exit(1)
        except Exception as other:
            logger.debug('Fatal exception encountered: %s', other)

        # Filter ILM indices unless expressly permitted
        if ilm_action_skip(client, action_def):
            continue
        #
        # Process the action
        #
        msg = (
            f'Trying Action ID: {idx}, "{action_def.action}": {action_def.description}'
        )
        try:
            logger.info(msg)
            process_action(client, action_def, dry_run=ctx.params['dry_run'])
        except Exception as err:
            exception_handler(action_def, err)
        logger.info('Action ID: %s, "%s" completed.', idx, action_def.action)
    logger.info('All actions completed.')


@click.command(
    context_settings=context_settings(),
    epilog=footer(__version__, tail='command-line.html'),
)
@options_from_dict(OPTION_DEFAULTS)
@click_opt_wrap(*cli_opts('dry-run', settings=CLICK_DRYRUN))
@click.argument('action_file', type=click.Path(exists=True), nargs=1)
@click.version_option(__version__, '-v', '--version', prog_name="curator")
@click.pass_context
def cli(
    ctx,
    config,
    hosts,
    cloud_id,
    api_token,
    id,
    api_key,
    username,
    password,
    bearer_auth,
    opaque_id,
    request_timeout,
    http_compress,
    verify_certs,
    ca_certs,
    client_cert,
    client_key,
    ssl_assert_hostname,
    ssl_assert_fingerprint,
    ssl_version,
    master_only,
    skip_version_test,
    loglevel,
    logfile,
    logformat,
    blacklist,
    dry_run,
    action_file,
):
    """
    Curator for Elasticsearch indices

    The default $HOME/.curator/curator.yml configuration file (--config)
    can be used but is not needed.

    Command-line settings will always override YAML configuration settings.

    Some less-frequently used client configuration options are now hidden. To see the
    full list,

    run:

        curator_cli -h
    """
    ctx.obj = {}
    ctx.obj['default_config'] = default_config_file()
    get_config(ctx)
    configure_logging(ctx)
    generate_configdict(ctx)
    run(ctx)
