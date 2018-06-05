import click
from curator.cli_singletons.object_class import cli_action
from curator.cli_singletons.utils import get_width, validate_filter_json
from curator.utils import byte_size
from datetime import datetime

#### Indices ####
@click.command(context_settings=get_width())
@click.option('--verbose', help='Show verbose output.', is_flag=True, show_default=True)
@click.option('--header', help='Print header if --verbose', is_flag=True, show_default=True)
@click.option('--epoch', help='Print time as epoch if --verbose', is_flag=True, show_default=True)
@click.option('--ignore_empty_list', is_flag=True, help='Do not raise exception if there are no actionable indices')
@click.option('--allow_ilm_indices/--no-allow_ilm_indices', help='Allow Curator to operate on Index Lifecycle Management monitored indices.', default=False, show_default=True)
@click.option('--filter_list', callback=validate_filter_json, default='{"filtertype":"none"}', help='JSON string representing an array of filters.')
@click.pass_context
def show_indices(ctx, verbose, header, epoch, ignore_empty_list, allow_ilm_indices, filter_list):
    """
    Show Indices
    """
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(ctx.info_name, ctx.obj['config']['client'], {'allow_ilm_indices': allow_ilm_indices}, filter_list, ignore_empty_list)
    action.get_list_object()
    action.do_filters()
    indices = sorted(action.list_object.indices)
    # Do some calculations to figure out the proper column sizes
    allbytes = []
    alldocs = []
    for idx in indices:
        allbytes.append(byte_size(action.list_object.index_info[idx]['size_in_bytes']))
        alldocs.append(str(action.list_object.index_info[idx]['docs']))
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
        p = action.list_object.index_info[idx]
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

#### Snapshots ####
@click.command(context_settings=get_width())
@click.option('--repository', type=str, required=True, help='Snapshot repository name')
@click.option('--ignore_empty_list', is_flag=True, help='Do not raise exception if there are no actionable snapshots')
@click.option('--filter_list', callback=validate_filter_json, default='{"filtertype":"none"}', help='JSON string representing an array of filters.')
@click.pass_context
def show_snapshots(ctx, repository, ignore_empty_list, filter_list):
    """
    Show Snapshots
    """
    # ctx.info_name is the name of the function or name specified in @click.command decorator
    action = cli_action(ctx.info_name, ctx.obj['config']['client'], {}, filter_list, ignore_empty_list, repository=repository)
    action.get_list_object()
    action.do_filters()
    for snapshot in sorted(action.list_object.snapshots):
        click.secho('{0}'.format(snapshot))
