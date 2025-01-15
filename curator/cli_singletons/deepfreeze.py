"""Deepfreeze Singleton"""
from datetime import datetime

import click

from curator.cli_singletons.object_class import CLIAction

today = datetime.today()

@click.group()
def deepfreeze():
    """
    Deepfreeze command group
    """
    pass

@deepfreeze.command()
@click.option(
    "--year", type=int, default=today.year, help="Year for the new repo"
)
@click.option(
    "--month", type=int, default=today.month, help="Month for the new repo"
)
@click.option(
    "--repo_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming rotating repositories",
)
@click.option(
    "--bucket_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming buckets",
)
@click.option(
    "--base_path",
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
@click.pass_context
def setup(
    ctx,
    year,
    month,
    repo_name_prefix,
    bucket_name_prefix,
    base_path,
    canned_acl,
    storage_class,
):
    """
    Setup a cluster for deepfreeze
    """
    manual_options = {
        'year': year,
        'month': month,
        'repo_name_prefix': repo_name_prefix,
        'bucket_name_prefix': bucket_name_prefix,
        'base_path': base_path,
        'canned_acl': canned_acl,
        'storage_class': storage_class,
    }

    pass

@deepfreeze.command()
@click.option(
    "--year", type=int, default=today.year, help="Year for the new repo"
)
@click.option(
    "--month", type=int, default=today.month, help="Month for the new repo"
)
@click.option(
    "--repo_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming rotating repositories",
)
@click.option(
    "--bucket_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming buckets",
)
@click.option(
    "--base_path",
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
    repo_name_prefix,
    bucket_name_prefix,
    base_path,
    canned_acl,
    storage_class,
    keep,
):
    """
    Deepfreeze rotation (add a new repo and age oldest off)
    """
    manual_options = {
        'year': year,
        'month': month,
        'repo_name_prefix': repo_name_prefix,
        'bucket_name_prefix': bucket_name_prefix,
        'base_path': base_path,
        'canned_acl': canned_acl,
        'storage_class': storage_class,
        'keep': keep,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj['configdict'],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])

@deepfreeze.command()
@click.option(
    "--start", type=click.DateTime(formats=["%Y-%m-%d"]), help="Start of period to be thawed"
)
@click.option(
    "--end", type=click.DateTime(formats=["%Y-%m-%d"]), help="End of period to be thawed"
)
@click.option(
    "--enable-multiple-buckets", is_flag=True, help="Enable multiple buckets for thawing if period spans multiple buckets"
)
@click.pass_context
def thaw(
    ctx,
    start,
    end,
    enable_multiple_buckets,
):
    """
    Thaw a deepfreeze repository
    """
    manual_options = {
        'start': start,
        'end': end,
        'enable_multiple_buckets': enable_multiple_buckets,
    }
    pass

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
        'thaw_set': thaw_set,
    }
    pass