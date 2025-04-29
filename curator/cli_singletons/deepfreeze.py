"""Deepfreeze Singleton"""

import logging
from datetime import datetime

import click

from curator.cli_singletons.object_class import CLIAction

today = datetime.today()


@click.group()
def deepfreeze():
    """
    Deepfreeze command group
    """


@deepfreeze.command()
@click.option(
    "-y",
    "--year",
    type=int,
    default=today.year,
    show_default=True,
    help="Year for the new repo. Only used if style=date.",
)
@click.option(
    "-m",
    "--month",
    type=int,
    default=today.month,
    show_default=True,
    help="Month for the new repo. Only used if style=date.",
)
@click.option(
    "-r",
    "--repo_name_prefix",
    type=str,
    default="deepfreeze",
    show_default=True,
    help="prefix for naming rotating repositories",
)
@click.option(
    "-b",
    "--bucket_name_prefix",
    type=str,
    default="deepfreeze",  
    show_default=True,
    help="prefix for naming buckets",
)
@click.option(
    "-p",
    "--base_path_prefix",
    type=str,
    default="snapshots",
    show_default=True,
    help="base path in the bucket to use for searchable snapshots",
)
@click.option(
    "-a",
    "--canned_acl",
    type=click.Choice(
        [
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "log-delivery-write",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
    ),
    default="private",
    show_default=True,
    help="Canned ACL as defined by AWS",
)
@click.option(
    "-s",
    "--storage_class",
    type=click.Choice(
        [
            "standard",
            "reduced_redundancy",
            "standard_ia",
            "intelligent_tiering",
            "onezone_ia",
        ]
    ),
    default="standard",
    show_default=True,
    help="What storage class to use, as defined by AWS",
)
@click.option(
    "-o",
    "--provider",
    type=click.Choice(
        [
            "aws",
            # "gcp",
            # "azure",
        ]
    ),
    default="aws",
    help="What provider to use (AWS only for now)",
)
@click.option(
    "-t",
    "--rotate_by",
    type=click.Choice(
        [
        #    "bucket",
            "path",
        ]
    ),
    default="path",
    help="Rotate by path. This is the only option available for now",
#    help="Rotate by bucket or path within a bucket?",
)
@click.option(
    "-n",
    "--style",
    type=click.Choice(
        [
            # "date",
            "oneup",
        ]
    ),
    default="oneup",
    help="How to number (suffix) the rotating repositories. Oneup is the only option available for now.",
    # help="How to number (suffix) the rotating repositories",
)
@click.option(
    "-c",
    "--create_sample_ilm_policy",
    is_flag=True,
    default=False,
    show_default=True,
    help="Create a sample ILM policy",
)
@click.option(
    "-i",
    "--ilm_policy_name",
    type=str,
    show_default=True,
    default="deepfreeze-sample-policy",
    help="Name of the sample ILM policy",
)
@click.pass_context
def setup(
    ctx,
    year,
    month,
    repo_name_prefix,
    bucket_name_prefix,
    base_path_prefix,
    canned_acl,
    storage_class,
    provider,
    rotate_by,
    style,
    create_sample_ilm_policy,
    ilm_policy_name,
):
    """
    Set up a cluster for deepfreeze and save the configuration for all future actions.

    Setup can be tuned by setting the following options to override defaults. Note that
    --year and --month are only used if style=date. If style=oneup, then year and month
    are ignored.

    Depending on the S3 provider chosen, some options might not be available, or option
    values may vary.
    """
    logging.debug("setup")
    manual_options = {
        "year": year,
        "month": month,
        "repo_name_prefix": repo_name_prefix,
        "bucket_name_prefix": bucket_name_prefix,
        "base_path_prefix": base_path_prefix,
        "canned_acl": canned_acl,
        "storage_class": storage_class,
        "provider": provider,
        "rotate_by": rotate_by,
        "style": style,
        "create_sample_ilm_policy": create_sample_ilm_policy,
        "ilm_policy_name": ilm_policy_name,
    }

    action = CLIAction(
        ctx.info_name,
        ctx.obj["configdict"],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj["dry_run"])


@deepfreeze.command()
@click.option(
    "-y",
    "--year",
    type=int,
    default=today.year,
    help="Year for the new repo (default is today)",
)
@click.option(
    "-m",
    "--month",
    type=int,
    default=today.month,
    help="Month for the new repo (default is today)",
)
@click.option(
    "-k",
    "--keep",
    type=int,
    default=6,
    help="How many repositories should remain mounted?",
)
@click.pass_context
def rotate(
    ctx,
    year,
    month,
    keep,
):
    """
    Deepfreeze rotation (add a new repo and age oldest off)
    """
    manual_options = {
        "year": year,
        "month": month,
        "keep": keep,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj["configdict"],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj["dry_run"])


@deepfreeze.command()
@click.option(
    "-s",
    "--start",
    type=click.STRING,
    required=True,
    help="Start of period to be thawed",
)
@click.option(
    "-e",
    "--end",
    type=click.STRING,
    required=True,
    help="End of period to be thawed",
)
@click.option(
    "-r",
    "--retain",
    type=int,
    default=7,
    help="How many days to retain the thawed repository",
)
@click.option(
    "-c",
    "--storage_class",
    type=click.Choice(
        [
            "standard",
            "reduced_redundancy",
            "standard_ia",
            "intelligent_tiering",
            "onezone_ia",
        ]
    ),
    default="intelligent_tiering",
    help="What storage class to use, as defined by AWS",
)
@click.option(
    "-w",
    "--wait_for_completion",
    is_flag=True,
    help="Wait for completion of the thaw",
)
@click.option(
    "-i",
    "--wait_interval",
    type=int,
    default=60,
    help="How often to check for completion of the thaw",
)
@click.option(
    "-m",
    "--max_wait",
    type=int,
    default=-1,
    help="How long to wait for completion of the thaw (-1 means forever)",
)
@click.option(
    "-m",
    "--enable-multiple-buckets",
    is_flag=True,
    help="Enable multiple buckets for thawing if period spans multiple buckets",
)
@click.pass_context
def thaw(
    ctx,
    start,
    end,
    retain,
    storage_class,
    wait_for_completion,
    wait_interval,
    max_wait,
    enable_multiple_buckets,
):
    """
    Thaw a deepfreeze repository (return it from Glacier)

    Specifying wait_for_completion will cause the CLI to wait for the thaw to complete
    and then proceed directly to remount the repository. This is useful for scripting
    the thaw process or unattended operation. This mode is the default, so you must
    specify --no-wait-for-completion to disable it.
    """
    manual_options = {
        "start": start,
        "end": end,
        "retain": retain,
        "storage_class": storage_class,
        "wait_for_completion": wait_for_completion,
        "wait_interval": wait_interval,
        "max_wait": max_wait,
        "enable_multiple_buckets": enable_multiple_buckets,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj["configdict"],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj["dry_run"])


@deepfreeze.command()
@click.option("-t", "--thawset", type=int, help="Thaw set with repos to be mounted.")
@click.option(
    "-w",
    "--wait_for_completion",
    is_flag=True,
    help="Wait for completion of the thaw",
)
@click.option(
    "-i",
    "--wait_interval",
    type=int,
    default=60,
    help="How often to check for completion of the thaw",
)
@click.option(
    "-m",
    "--max_wait",
    type=int,
    default=-1,
    help="How long to wait for completion of the thaw (-1 means forever)",
)
@click.pass_context
def remount(
    ctx,
    thawset,
    wait_for_completion,
    wait_interval,
    max_wait,
):
    """
    Remount a thawed repository
    """
    manual_options = {
        "thawset": thawset,
        "wait_for_completion": wait_for_completion,
        "wait_interval": wait_interval,
        "max_wait": max_wait,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj["configdict"],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj["dry_run"])


@deepfreeze.command()
@click.option(
    "-t",
    "--thawset",
    type=int,
    help="Thaw set to be re-frozen. If omitted, re-freeze all.",
)
@click.pass_context
def refreeze(
    ctx,
    thawset,
):
    """
    Refreeze a thawed repository
    """
    manual_options = {
        "thawset": thawset,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj["configdict"],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj["dry_run"])


@deepfreeze.command()
@click.pass_context
def status(
    ctx,
):
    """
    Show the status of deepfreeze
    """
    manual_options = {}
    action = CLIAction(
        ctx.info_name,
        ctx.obj["configdict"],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj["dry_run"])
