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
    "--year",
    type=int,
    default=today.year,
    help="Year for the new repo (default is today)",
)
@click.option(
    "--month",
    type=int,
    default=today.month,
    help="Month for the new repo (default is today)",
)
@click.option(
    "--repo_name_prefix",
    type=str,
    default="deepfreeze",
    help="prefix for naming rotating repositories",
)
@click.option(
    "--bucket_name_prefix",
    type=str,
    default="deepfreeze",
    help="prefix for naming buckets",
)
@click.option(
    "--base_path_prefix",
    type=str,
    default="snapshots",
    help="base path in the bucket to use for searchable snapshots",
)
@click.option(
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
    help="Canned ACL as defined by AWS",
)
@click.option(
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
    "--rotate_by",
    type=click.Choice(
        [
            "bucket",
            "path",
        ]
    ),
    default="path",
    help="Rotate by bucket or path within a bucket?",
)
@click.option(
    "--style",
    type=click.Choice(
        [
            "date",
            "oneup",
        ]
    ),
    default="oneup",
    help="How to number (suffix) the rotating repositories",
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
):
    """
    Setup a cluster for deepfreeze
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
    "--year",
    type=int,
    default=today.year,
    help="Year for the new repo (default is today)",
)
@click.option(
    "--month",
    type=int,
    default=today.month,
    help="Month for the new repo (default is today)",
)
@click.option(
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
    enable_multiple_buckets,
):
    """
    Thaw a deepfreeze repository
    """
    manual_options = {
        "start": start,
        "end": end,
        "retain": retain,
        "storage_class": storage_class,
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
@click.option("--thaw-set", type=int, help="Thaw set with repos to be mounted.")
@click.pass_context
def remount(
    ctx,
    thaw_set,
):
    """
    Remount a thawed repository
    """
    manual_options = {
        "thaw_set": thaw_set,
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
    "--thaw-set", type=int, help="Thaw set to be re-frozen. If omitted, re-freeze all."
)
@click.pass_context
def refreeze(
    ctx,
    thaw_set,
):
    """
    Refreeze a thawed repository
    """
    manual_options = {
        "thaw_set": thaw_set,
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
