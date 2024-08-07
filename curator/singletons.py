"""CLI module for curator_cli"""

import click
from es_client.defaults import SHOW_EVERYTHING
from es_client.helpers.config import (
    cli_opts,
    context_settings,
    generate_configdict,
    get_config,
    options_from_dict,
)
from es_client.helpers.logging import configure_logging
from es_client.helpers.utils import option_wrapper
from curator.defaults.settings import CLICK_DRYRUN, default_config_file, footer
from curator._version import __version__
from curator.cli_singletons import (
    alias,
    allocation,
    close,
    delete_indices,
    delete_snapshots,
    forcemerge,
    open_indices,
    replicas,
    restore,
    rollover,
    snapshot,
    shrink,
)
from curator.cli_singletons.show import show_indices, show_snapshots

click_opt_wrap = option_wrapper()


# pylint: disable=R0913, R0914, W0613, W0622, W0718
@click.group(
    context_settings=context_settings(),
    epilog=footer(__version__, tail='singleton-cli.html'),
)
@options_from_dict(SHOW_EVERYTHING)
@click_opt_wrap(*cli_opts('dry-run', settings=CLICK_DRYRUN))
@click.version_option(__version__, '-v', '--version', prog_name='curator_cli')
@click.pass_context
def curator_cli(
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
):
    """
    Curator CLI (Singleton Tool)

    Run a single action from the command-line.

    The default $HOME/.curator/curator.yml configuration file (--config)
    can be used but is not needed.

    Command-line settings will always override YAML configuration settings.
    """
    ctx.obj = {}
    ctx.obj['dry_run'] = dry_run
    ctx.obj['default_config'] = default_config_file()
    get_config(ctx)
    configure_logging(ctx)
    generate_configdict(ctx)


# Add the subcommands
curator_cli.add_command(alias)
curator_cli.add_command(allocation)
curator_cli.add_command(close)
curator_cli.add_command(delete_indices)
curator_cli.add_command(delete_snapshots)
curator_cli.add_command(forcemerge)
curator_cli.add_command(open_indices)
curator_cli.add_command(replicas)
curator_cli.add_command(snapshot)
curator_cli.add_command(restore)
curator_cli.add_command(rollover)
curator_cli.add_command(shrink)
curator_cli.add_command(show_indices)
curator_cli.add_command(show_snapshots)
