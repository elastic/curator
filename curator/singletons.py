"""CLI module for curator_cli"""
import click
from es_client.defaults import LOGGING_SETTINGS, SHOW_OPTION
from es_client.helpers.config import cli_opts, context_settings, get_config, get_args
from es_client.helpers.logging import configure_logging
from es_client.helpers.utils import option_wrapper, prune_nones
from curator.defaults.settings import CLICK_DRYRUN, default_config_file, footer
from curator._version import __version__
from curator.cli_singletons import (
    alias, allocation, close, delete_indices, delete_snapshots, forcemerge, open_indices, replicas,
    restore, rollover, snapshot, shrink
)
from curator.cli_singletons.show import show_indices, show_snapshots

ONOFF = {'on': '', 'off': 'no-'}
click_opt_wrap = option_wrapper()

# pylint: disable=unused-argument, redefined-builtin, too-many-arguments, too-many-locals
@click.group(
    context_settings=context_settings(), epilog=footer(__version__, tail='singleton-cli.html'))
@click_opt_wrap(*cli_opts('config'))
@click_opt_wrap(*cli_opts('hosts'))
@click_opt_wrap(*cli_opts('cloud_id'))
@click_opt_wrap(*cli_opts('api_token'))
@click_opt_wrap(*cli_opts('id'))
@click_opt_wrap(*cli_opts('api_key'))
@click_opt_wrap(*cli_opts('username'))
@click_opt_wrap(*cli_opts('password'))
@click_opt_wrap(*cli_opts('bearer_auth', override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('opaque_id', override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('request_timeout'))
@click_opt_wrap(*cli_opts('http_compress', onoff=ONOFF, override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('verify_certs', onoff=ONOFF))
@click_opt_wrap(*cli_opts('ca_certs'))
@click_opt_wrap(*cli_opts('client_cert'))
@click_opt_wrap(*cli_opts('client_key'))
@click_opt_wrap(*cli_opts('ssl_assert_hostname', override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('ssl_assert_fingerprint', override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('ssl_version', override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('master-only', onoff=ONOFF, override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('skip_version_test', onoff=ONOFF, override=SHOW_OPTION))
@click_opt_wrap(*cli_opts('dry-run', settings=CLICK_DRYRUN))
@click_opt_wrap(*cli_opts('loglevel', settings=LOGGING_SETTINGS))
@click_opt_wrap(*cli_opts('logfile', settings=LOGGING_SETTINGS))
@click_opt_wrap(*cli_opts('logformat', settings=LOGGING_SETTINGS))
@click.version_option(__version__, '-v', '--version', prog_name='curator_cli')
@click.pass_context
def curator_cli(
    ctx, config, hosts, cloud_id, api_token, id, api_key, username, password, bearer_auth,
    opaque_id, request_timeout, http_compress, verify_certs, ca_certs, client_cert, client_key,
    ssl_assert_hostname, ssl_assert_fingerprint, ssl_version, master_only, skip_version_test,
    dry_run, loglevel, logfile, logformat
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
    cfg = get_config(ctx.params, default_config_file())
    configure_logging(cfg, ctx.params)
    client_args, other_args = get_args(ctx.params, cfg)
    final_config = {
        'elasticsearch': {
            'client': prune_nones(client_args.asdict()),
            'other_settings': prune_nones(other_args.asdict())
        }
    }
    ctx.obj['config'] = final_config

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
