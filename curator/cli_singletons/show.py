"""Show Index/Snapshot Singletons"""

from datetime import datetime
import click
from curator.cli_singletons.object_class import CLIAction
from curator.cli_singletons.utils import validate_filter_json
from curator.helpers.getters import byte_size
from curator.defaults.settings import footer
from curator._version import __version__

# ### Indices ###


# pylint: disable=line-too-long,W0718
@click.command(
    epilog=footer(__version__, tail='singleton-cli.html#_show_indicessnapshots')
)
@click.option(
    '--search_pattern', type=str, default='*', help='Elasticsearch Index Search Pattern'
)
@click.option('--verbose', help='Show verbose output.', is_flag=True, show_default=True)
@click.option(
    '--header', help='Print header if --verbose', is_flag=True, show_default=True
)
@click.option(
    '--epoch', help='Print time as epoch if --verbose', is_flag=True, show_default=True
)
@click.option(
    '--ignore_empty_list',
    is_flag=True,
    help='Do not raise exception if there are no actionable indices',
)
@click.option(
    '--allow_ilm_indices/--no-allow_ilm_indices',
    help='Allow Curator to operate on Index Lifecycle Management monitored indices.',
    default=False,
    show_default=True,
)
@click.option(
    '--include_datastreams/--no-include_datastreams',
    help='Allow Curator to operate on data streams.',
    default=False,
    show_default=True,
)
@click.option(
    '--include_hidden/--no-include_hidden',
    help='Allow Curator to operate on hidden indices (and data_streams).',
    default=False,
    show_default=True,
)
@click.option(
    '--include_kibana/--no-include_kibana',
    help='Allow Curator to operate on Kibana indices.',
    default=False,
    show_default=True,
)
@click.option(
    '--include_system/--no-include_system',
    help='Allow Curator to operate on system indices.',
    default=False,
    show_default=True,
)
@click.option(
    '--filter_list',
    callback=validate_filter_json,
    default='{"filtertype":"none"}',
    help='JSON string representing an array of filters.',
)
@click.pass_context
def show_indices(
    ctx,
    search_pattern,
    verbose,
    header,
    epoch,
    ignore_empty_list,
    allow_ilm_indices,
    include_datastreams,
    include_kibana,
    include_system,
    include_hidden,
    filter_list,
):
    """
    Show Indices
    """
    # ctx.info_name is the name of the function or name specified in
    # @click.command decorator
    click.secho(f'allow_ilm_indices: {allow_ilm_indices}', fg='yellow')
    action = CLIAction(
        'show_indices',
        ctx.obj['configdict'],
        {
            'search_pattern': search_pattern,
            'allow_ilm_indices': allow_ilm_indices,
            'include_datastreams': include_datastreams,
            'include_hidden': include_hidden,
            'include_kibana': include_kibana,
            'include_system': include_system,
        },
        filter_list,
        ignore_empty_list,
    )
    action.get_list_object()
    action.do_filters()
    # Gather index stats for all indices in the list, after filtering
    action.list_object.get_index_stats()  # type: ignore
    indices = sorted(action.list_object.indices)  # type: ignore
    allbytes = []
    alldocs = []
    for idx in indices:
        try:
            allbytes.append(
                byte_size(action.list_object.index_info[idx]['size_in_bytes'])  # type: ignore
            )
        except Exception as exc:
            allbytes.append('0B')
            click.echo(f'Error getting size for {idx}: {exc}')
        try:
            alldocs.append(str(action.list_object.index_info[idx]['docs']))  # type: ignore
        except Exception as exc:
            alldocs.append('0')
            click.echo(f'Error getting doc count for {idx}: {exc}')
    # Do some calculations to figure out the proper column sizes
    # Set up formatting strings based on the longest values for each column
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
            formatting.format('Index', 'State', 'Size', 'Docs', 'Pri', 'Rep', column),
            bold=True,
            underline=True,
        )
    # Loop through indices and print info, if verbose
    for idx in indices:
        data = action.list_object.index_info[idx]  # type: ignore
        if verbose:
            if epoch:
                datefield = (
                    data['age']['creation_date']
                    if 'creation_date' in data['age']
                    else 0
                )
            else:
                datefield = (
                    datetime.utcfromtimestamp(data['age']['creation_date']).isoformat()
                    if 'creation_date' in data['age']
                    else 'unknown/closed'
                )
            click.echo(
                formatting.format(
                    idx,
                    data['state'],
                    byte_size(data['size_in_bytes']),
                    data['docs'],
                    data['number_of_shards'],
                    data['number_of_replicas'],
                    f'{datefield}Z',
                )
            )
        else:
            click.secho(f'{idx}')


# ### Snapshots ###


# pylint: disable=line-too-long
@click.command(
    epilog=footer(__version__, tail='singleton-cli.html#_show_indicessnapshots')
)
@click.option('--repository', type=str, required=True, help='Snapshot repository name')
@click.option(
    '--ignore_empty_list',
    is_flag=True,
    help='Do not raise exception if there are no actionable snapshots',
)
@click.option(
    '--filter_list',
    callback=validate_filter_json,
    default='{"filtertype":"none"}',
    help='JSON string representing an array of filters.',
)
@click.pass_context
def show_snapshots(ctx, repository, ignore_empty_list, filter_list):
    """
    Show Snapshots
    """
    # ctx.info_name is the name of the function or name specified in
    # @click.command decorator
    action = CLIAction(
        'show_snapshots',
        ctx.obj['configdict'],
        {},
        filter_list,
        ignore_empty_list,
        repository=repository,
    )
    action.get_list_object()
    action.do_filters()
    for snapshot in sorted(action.list_object.snapshots):  # type: ignore
        click.secho(f'{snapshot}')
