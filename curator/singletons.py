import os, sys
import yaml, json
import logging
import click
from voluptuous import Schema
from .defaults import settings
from .validators import SchemaCheck, config_file, options
from .config_utils import test_config, set_logging
from .exceptions import *
from .utils import *
from .indexlist import IndexList
from .snapshotlist import SnapshotList
from .actions import *
from ._version import __version__

CLASS_MAP = {
    'alias' :  Alias,
    'allocation' : Allocation,
    'close' : Close,
    'cluster_routing' : ClusterRouting,
    'create_index' : CreateIndex,
    'delete_indices' : DeleteIndices,
    'delete_snapshots' : DeleteSnapshots,
    'forcemerge' : ForceMerge,
    'open' : Open,
    'replicas' : Replicas,
    'restore' : Restore,
    'snapshot' : Snapshot,
}

EXCLUDED_OPTIONS = [
    'ignore_empty_list', 'timeout_override',
    'continue_if_exception', 'disable_action'
]

def validate_filter_json(ctx, param, value):
    try:
        filter_list = ensure_list(json.loads(value))
        return filter_list
    except ValueError:
        raise click.BadParameter('Invalid JSON: {0}'.format(value))

def false_to_none(ctx, param, value):
    try:
        if value:
            return True
        else:
            return None
    except ValueError:
        raise click.BadParameter('Invalid value: {0}'.format(value))

def filter_schema_check(action, filter_dict):
    valid_filters = SchemaCheck(
        filter_dict,
        Schema(filters.Filters(action, location='singleton')),
        'filters',
        '{0} singleton action "filters"'.format(action)
    ).result()
    return validate_filters(action, valid_filters)

def _actionator(action, action_obj, dry_run=True):
    logger = logging.getLogger(__name__)
    logger.debug('Doing the singleton "{0}" action here.'.format(action))
    try:
        if dry_run:
            action_obj.do_dry_run()
        else:
            action_obj.do_action()
    except Exception as e:
        if isinstance(e, NoIndices) or isinstance(e, NoSnapshots):
            logger.error(
                'Unable to complete action "{0}".  No actionable items '
                'in list: {1}'.format(action, type(e))
            )
        else:
            logger.error(
                'Failed to complete action: {0}.  {1}: '
                '{2}'.format(action, type(e), e)
            )
        sys.exit(1)
    logger.info('Singleton "{0}" action completed.'.format(action))

def _do_filters(list_object, filters, ignore=False):
    logger = logging.getLogger(__name__)
    logger.debug('Running filters and testing for empty list object')
    try:
        list_object.iterate_filters(filters)
        list_object.empty_list_check()
    except (NoIndices, NoSnapshots) as e:
        if isinstance(e, NoIndices):
            otype = 'index'
        else:
            otype = 'snapshot'
        if ignore:
            logger.info(
                'Singleton action not performed: empty {0} list'.format(otype)
            )
            sys.exit(0)
        else:
            logger.error(
                'Singleton action failed due to empty {0} list'.format(otype)
            )
            sys.exit(1)


def _prune_excluded(option_dict):
    for k in list(option_dict.keys()):
        if k in EXCLUDED_OPTIONS:
            del option_dict[k]
    return option_dict

def option_schema_check(action, option_dict):
    clean_options = SchemaCheck(
        prune_nones(option_dict),
        options.get_schema(action),
        'options',
        '{0} singleton action "options"'.format(action)
    ).result()
    return _prune_excluded(clean_options)

def config_override(ctx, config_dict):
    if config_dict == None:
        config_dict = {}
    for k in ['client', 'logging']:
        if not k in config_dict:
            config_dict[k] = {}
    for k in list(ctx.params.keys()):
        if k in ['dry_run', 'config']:
            pass
        elif k == 'host':
            if 'host' in ctx.params and ctx.params['host'] is not None:
                config_dict['client']['hosts'] = ctx.params[k]
        elif k in ['loglevel', 'logfile', 'logformat']:
            if k in ctx.params and ctx.params[k] is not None:
                config_dict['logging'][k] = ctx.params[k]
        else:
            if k in ctx.params and ctx.params[k] is not None:
                config_dict['client'][k] = ctx.params[k]
    # After override, prune the nones
    for k in ['client', 'logging']:
        config_dict[k] = prune_nones(config_dict[k])
    return SchemaCheck(config_dict, config_file.client(),
        'Client Configuration', 'full configuration dictionary').result()

@click.command(name='allocation')
@click.option(
    '--key', type=str, required=True, help='Node identification tag'
)
@click.option(
    '--value', type=str, default=None, help='Value associated with --key'
)
@click.option(
    '--allocation_type', type=str,
    help='Must be one of: require, include, or exclude'
)
@click.option(
    '--wait_for_completion', is_flag=True, help='Wait for operation to complete'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json,
    help='JSON string representing an array of filters.', required=True
)
@click.pass_context
def allocation_singleton(
    ctx, key, value, allocation_type, wait_for_completion, ignore_empty_list,
    filter_list):
    """
    Shard Routing Allocation
    """
    action = 'allocation'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    raw_options = {
        'key': key,
        'value': value,
        'allocation_type': allocation_type,
        'wait_for_completion': wait_for_completion,
    }
    logger.debug('Validating provided options: {0}'.format(raw_options))
    mykwargs = option_schema_check(action, raw_options)
    mykwargs.update(
        { 'max_wait': c_args['timeout'] if c_args['timeout'] else 30 }
    )
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='close')
@click.option(
    '--delete_aliases', is_flag=True,
    help='Delete all aliases from indices to be closed'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json,
    help='JSON string representing an array of filters.', required=True
)
@click.pass_context
def close_singleton(
    ctx, delete_aliases, ignore_empty_list, filter_list):
    """
    Close indices
    """
    action = 'close'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    raw_options = { 'delete_aliases': delete_aliases }
    logger.debug('Validating provided options: {0}'.format(raw_options))
    mykwargs = option_schema_check(action, raw_options)
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='delete_indices')
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json,
    help='JSON string representing an array of filters.', required=True
)
@click.pass_context
def delete_indices_singleton(ctx, ignore_empty_list, filter_list):
    """
    Delete indices
    """
    action = 'delete_indices'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    mykwargs = {
        'master_timeout': c_args['timeout'] if c_args['timeout'] <= 300 else 300
    }
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='delete_snapshots')
@click.option(
    '--repository', type=str, required=True, help='Snapshot repository name'
)
@click.option(
    '--retry_count', type=int, help='Number of times to retry (max 3)'
)
@click.option(
    '--retry_interval', type=int, help='Time in seconds between retries'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable snapshots'
)
@click.option(
    '--filter_list', callback=validate_filter_json,
    help='JSON string representing an array of filters.', required=True
)
@click.pass_context
def delete_snapshots_singleton(
    ctx, repository, retry_count, retry_interval, ignore_empty_list,
    filter_list):
    """
    Delete snapshots
    """
    action = 'delete_snapshots'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    raw_options = {
        'repository': repository,
        'retry_count': retry_count, 'retry_interval': retry_interval
    }
    logger.debug('Validating provided options: {0}'.format(raw_options))
    mykwargs = option_schema_check(action, raw_options)
    # Repo arg Not necessary after schema check.  It's only for the slo object
    del mykwargs['repository']
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    slo = SnapshotList(client, repository=repository)
    _do_filters(slo, clean_filters, ignore_empty_list)
    action_obj = action_class(slo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='open')
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json,
    help='JSON string representing an array of filters.', required=True
)
@click.pass_context
def open_singleton(
    ctx, ignore_empty_list, filter_list):
    """
    Open indices
    """
    action = 'open'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    action_obj = action_class(ilo)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='forcemerge')
@click.option(
    '--max_num_segments', type=int, required=True,
    help='Maximum number of segments per shard (minimum of 1)'
)
@click.option(
    '--delay', type=float,
    help='Time in seconds to delay between operations. Default 0, maximum 3600'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json,
    help='JSON string representing an array of filters.', required=True
)
@click.pass_context
def forcemerge_singleton(
    ctx, max_num_segments, delay, ignore_empty_list, filter_list):
    """
    forceMerge index/shard segments
    """
    action = 'forcemerge'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    raw_options = {
        'max_num_segments': max_num_segments,
        'delay': delay,
    }
    logger.debug('Validating provided options: {0}'.format(raw_options))
    mykwargs = option_schema_check(action, raw_options)
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='replicas')
@click.option(
    '--count', type=int, required=True, help='Number of replicas (max 10)'
)
@click.option(
    '--wait_for_completion', is_flag=True, help='Wait for operation to complete'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json,
    help='JSON string representing an array of filters.', required=True
)
@click.pass_context
def replicas_singleton(
    ctx, count, wait_for_completion, ignore_empty_list, filter_list):
    """
    Change replica count
    """
    action = 'replicas'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    raw_options = {
        'count': count,
        'wait_for_completion': wait_for_completion,
    }
    logger.debug('Validating provided options: {0}'.format(raw_options))
    mykwargs = option_schema_check(action, raw_options)
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='snapshot')
@click.option(
    '--repository', type=str, required=True, help='Snapshot repository')
@click.option(
    '--name', type=str, help='Snapshot name',
    show_default=True, default='curator-%Y%m%d%H%M%S'
)
@click.option(
    '--ignore_unavailable', is_flag=True, show_default=True,
    help='Ignore unavailable shards/indices.'
)
@click.option(
    '--include_global_state', type=bool, show_default=True,
    default=True, expose_value=True,
    help='Store cluster global state with snapshot.'
)
@click.option(
    '--partial', is_flag=True, show_default=True,
    help='Do not fail if primary shard is unavailable.'
)
@click.option(
    '--wait_for_completion',
    type=bool, show_default=True, default=True,
    help='Wait for operation to complete'
)
@click.option(
    '--skip_repo_fs_check', is_flag=True, expose_value=True,
    help='Skip repository filesystem access validation.'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json, default='{"filtertype":"none"}',
    help='JSON string representing an array of filters.'
)
@click.pass_context
def snapshot_singleton(
    ctx, repository, name, ignore_unavailable, include_global_state, partial,
    skip_repo_fs_check, wait_for_completion, ignore_empty_list, filter_list):
    """
    Snapshot indices
    """
    action = 'snapshot'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    raw_options = {
        'repository': repository,
        'name': name,
        'ignore_unavailable': ignore_unavailable,
        'include_global_state': include_global_state,
        'partial': partial,
        'skip_repo_fs_check': skip_repo_fs_check,
        'wait_for_completion': wait_for_completion,
    }
    logger.debug('Validating provided options: {0}'.format(raw_options))
    mykwargs = option_schema_check(action, raw_options)
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    action_obj = action_class(ilo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.command(name='show_indices')
@click.option('--verbose', help='Show verbose output.', is_flag=True)
@click.option('--header', help='Print header if --verbose', is_flag=True)
@click.option('--epoch', help='Print time as epoch if --verbose', is_flag=True)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json, default='{"filtertype":"none"}',
    help='JSON string representing an array of filters.'
)
@click.pass_context
def show_indices_singleton(
    ctx, epoch, header, verbose, ignore_empty_list, filter_list):
    """
    Show indices
    """
    action = "open"
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    logger.debug(
        'Using dummy "open" action for show_indices singleton.  '
        'No action will be taken.'
    )
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    ilo = IndexList(client)
    _do_filters(ilo, clean_filters, ignore_empty_list)
    indices = sorted(ilo.indices)
    # Do some calculations to figure out the proper column sizes
    allbytes = []
    alldocs = []
    for idx in indices:
        allbytes.append(byte_size(ilo.index_info[idx]['size_in_bytes']))
        alldocs.append(str(ilo.index_info[idx]['docs']))
    if epoch:
        timeformat = '{6:>13}'
        column = 'creation_date'
    else:
        timeformat = '{6:>20}'
        column = 'Creation Timestamp'
    formatting = (
        '{0:' + str(len(max(indices, key=len))) + '} '
        '{1:>5} '
        '{2:>' + str(len(max(allbytes, key=len)) + 1) + '} '
        '{3:>' + str(len(max(alldocs, key=len)) + 1) + '} '
        '{4:>3} {5:>3} ' + timeformat
    )
    # Print the header, if both verbose and header are enabled
    if header and verbose:
        click.secho(
            formatting.format(
                'Index', 'State', 'Size', 'Docs', 'Pri', 'Rep', column
            ), bold=True, underline=True
        )
    # Loop through indices and print info, if verbose
    for idx in indices:
        p = ilo.index_info[idx]
        if verbose:
            if epoch:
                datefield = p['age']['creation_date'] if 'creation_date' in p['age'] else 0
            else:
                datefield = '{0}Z'.format(
                    datetime.utcfromtimestamp(p['age']['creation_date']
                ).isoformat()) if 'creation_date' in p['age'] else 'unknown/closed'
            click.echo(
                formatting.format(
                    idx, p['state'], byte_size(p['size_in_bytes']),
                    p['docs'], p['number_of_shards'], p['number_of_replicas'],
                    datefield
                )
            )
        else:
            click.echo('{0}'.format(idx))


@click.command(name='show_snapshots')
@click.option(
    '--repository', type=str, required=True, help='Snapshot repository name'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable snapshots'
)
@click.option(
    '--filter_list', callback=validate_filter_json, default='{"filtertype":"none"}',
    help='JSON string representing an array of filters.'
)
@click.pass_context
def show_snapshots_singleton(
    ctx, repository, ignore_empty_list, filter_list):
    """
    Show snapshots
    """
    action = 'delete_snapshots'
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    slo = SnapshotList(client, repository=repository)
    _do_filters(slo, clean_filters, ignore_empty_list)
    snapshots = sorted(slo.snapshots)
    for idx in snapshots:
        click.secho('{0}'.format(idx))


@click.command(name='restore')
@click.option(
    '--repository', type=str, required=True, help='Snapshot repository')
@click.option(
    '--name', type=str, help='Snapshot name', required=False, default=None,
)
@click.option(
    '--rename_pattern', type=str, help='Rename pattern', required=False, default=None,
)
@click.option(
    '--rename_replacement', type=str, help='Rename replacement', required=False, default=None,
)
@click.option(
    '--ignore_unavailable', is_flag=True, show_default=True,
    help='Ignore unavailable shards/indices.'
)
@click.option(
    '--include_global_state', type=bool, show_default=True,
    default=True, expose_value=True,
    help='Store cluster global state with snapshot.'
)
@click.option(
    '--partial', is_flag=True, show_default=True,
    help='Do not fail if primary shard is unavailable.'
)
@click.option(
    '--wait_for_completion',
    type=bool, show_default=True, default=True,
    help='Wait for operation to complete'
)
@click.option(
    '--skip_repo_fs_check', is_flag=True, expose_value=True,
    help='Skip repository filesystem access validation.'
)
@click.option(
    '--ignore_empty_list', is_flag=True,
    help='Do not raise exception if there are no actionable indices'
)
@click.option(
    '--filter_list', callback=validate_filter_json, default='{"filtertype":"none"}',
    help='JSON string representing an array of filters.'
)
@click.pass_context
def restore_singleton(
    ctx, repository, name, rename_pattern, rename_replacement, ignore_unavailable,
    include_global_state, partial, wait_for_completion, skip_repo_fs_check,
    ignore_empty_list, filter_list):
    """
    Restore a snapshot
    """
    action = 'restore'
    action_class = CLASS_MAP[action]
    c_args = ctx.obj['config']['client']
    client = get_client(**c_args)
    logger = logging.getLogger(__name__)
    raw_options = {
        'repository': repository,
        'name': name,
        'rename_pattern': rename_pattern,
        'rename_replacement': rename_replacement,
        'ignore_unavailable': ignore_unavailable,
        'include_global_state': include_global_state,
        'partial': partial,
        'skip_repo_fs_check': skip_repo_fs_check,
        'wait_for_completion': wait_for_completion,
    }
    logger.debug('Validating provided options: {0}'.format(raw_options))
    mykwargs = option_schema_check(action, raw_options)
    mykwargs.pop('repository')
    logger.debug('Validating provided filters: {0}'.format(filter_list))
    clean_filters = {
        'filters': filter_schema_check(action, filter_list)
    }
    slo = SnapshotList(client, repository=repository)
    _do_filters(slo, clean_filters, ignore_empty_list)
    action_obj = action_class(slo, **mykwargs)
    ### Do the action
    _actionator(action, action_obj, dry_run=ctx.parent.params['dry_run'])


@click.group()
@click.option(
    '--config',
    help='Path to configuration file. Default: ~/.curator/curator.yml',
    type=click.Path(), default=settings.config_file()
)
@click.option('--host', help='Elasticsearch host.')
@click.option('--url_prefix', help='Elasticsearch http url prefix.')
@click.option('--port', help='Elasticsearch port.')
@click.option(
    '--use_ssl', is_flag=True, callback=false_to_none,
    help='Connect to Elasticsearch through SSL.'
)
@click.option(
    '--certificate', help='Path to certificate to use for SSL validation.')
@click.option(
    '--client-cert',
    help='Path to file containing SSL certificate for client auth.', type=str
)
@click.option(
    '--client-key',
    help='Path to file containing SSL key for client auth.', type=str
)
@click.option(
    '--ssl-no-validate', is_flag=True, callback=false_to_none,
    help='Do not validate SSL certificate'
)
@click.option('--http_auth', help='Use Basic Authentication ex: user:pass')
@click.option('--timeout', help='Connection timeout in seconds.', type=int)
@click.option(
    '--master-only', is_flag=True, callback=false_to_none,
    help='Only operate on elected master node.'
)
@click.option('--dry-run', is_flag=True, help='Do not perform any changes.')
@click.option('--loglevel', help='Log level')
@click.option('--logfile', help='log file')
@click.option('--logformat', help='Log output format [default|logstash|json].')
@click.version_option(version=__version__)
@click.pass_context
def cli(
    ctx, config, host, url_prefix, port, use_ssl, certificate, client_cert,
    client_key, ssl_no_validate, http_auth, timeout, master_only, dry_run,
    loglevel, logfile, logformat):
    if os.path.isfile(config):
        initial_config = test_config(config)
    else:
        initial_config = None
    configuration = config_override(ctx, initial_config)
    set_logging(configuration['logging'])
    test_client_options(configuration['client'])
    logger = logging.getLogger(__name__)
    ctx.obj['config'] = configuration
cli.add_command(allocation_singleton)
cli.add_command(close_singleton)
cli.add_command(delete_indices_singleton)
cli.add_command(delete_snapshots_singleton)
cli.add_command(forcemerge_singleton)
cli.add_command(open_singleton)
cli.add_command(replicas_singleton)
cli.add_command(snapshot_singleton)
cli.add_command(restore_singleton)
cli.add_command(show_indices_singleton)
cli.add_command(show_snapshots_singleton)
