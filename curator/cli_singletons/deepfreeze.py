"""Deepfreeze Singleton"""
from datetime import datetime

import click

from curator.cli_singletons.object_class import CLIAction

today=datetime.today()
@click.command()
@click.option("--year", type=int, default=today.year, help="Year for the new repository")
@click.option("--month", type=int, default=today.month, help="Month for the new repository")
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
@click.option(
    '--setup',
    is_flag=True,
    help="Perform setup steps for an initial deepfreeze repository",
    default=False,
)
@click.pass_context
def deepfreeze(
    ctx,
    year,
    month,
    repo_name_prefix,
    bucket_name_prefix,
    base_path,
    canned_acl,
    storage_class,
    keep,
    setup,
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
        'setup': setup,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj['configdict'],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
