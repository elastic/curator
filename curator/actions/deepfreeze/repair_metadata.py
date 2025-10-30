"""Repair Metadata action for deepfreeze"""

# pylint: disable=too-many-instance-attributes

import logging

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.table import Table

from curator.actions.deepfreeze import STATUS_INDEX
from curator.actions.deepfreeze.utilities import get_repositories_by_names, get_settings
from curator.s3client import s3_client_factory


class RepairMetadata:
    """
    The RepairMetadata action scans all repositories and fixes metadata discrepancies
    where the status index shows incorrect thaw_state compared to actual S3 storage class.

    This action:
    1. Scans all repositories in the status index
    2. Checks actual S3 storage class for each repository
    3. Updates thaw_state='frozen' for repositories actually stored in GLACIER
    4. Reports on all changes made

    This is useful when metadata gets out of sync with reality, such as when:
    - Rotation pushes repos to GLACIER but doesn't update thaw_state
    - Manual S3 operations change storage class
    - Errors during state transitions

    :param client: A client connection object
    :type client: Elasticsearch
    :param porcelain: Output plain text without formatting
    :type porcelain: bool

    :methods:
        do_action: Perform the metadata repair operation.
        do_dry_run: Perform a dry-run showing what would be changed.
        do_singleton_action: Entry point for singleton CLI execution.
    """

    def __init__(
        self,
        client: Elasticsearch,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze RepairMetadata")

        self.client = client
        self.porcelain = porcelain
        self.settings = get_settings(client)
        self.s3_wrapper = s3_client_factory(self.settings.provider)
        self.s3 = self.s3_wrapper.client  # Get boto3 client

        self.loggit.info("Deepfreeze RepairMetadata initialized")

    def _check_repo_storage_class(self, bucket: str, base_path: str) -> str:
        """
        Check the actual S3 storage class for a repository.

        :param bucket: S3 bucket name
        :type bucket: str
        :param base_path: Base path in the bucket
        :type base_path: str

        :return: Storage class status: 'GLACIER', 'STANDARD', 'MIXED', or 'EMPTY'
        :rtype: str
        """
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=base_path, MaxKeys=100)

            glacier_count = 0
            standard_count = 0
            total_count = 0

            for page in pages:
                if 'Contents' not in page:
                    break
                for obj in page['Contents']:
                    total_count += 1
                    storage_class = obj.get('StorageClass', 'STANDARD')
                    if storage_class in ['GLACIER', 'DEEP_ARCHIVE', 'GLACIER_IR']:
                        glacier_count += 1
                    else:
                        standard_count += 1

            if total_count == 0:
                return 'EMPTY'
            if glacier_count == total_count:
                return 'GLACIER'
            if glacier_count > 0:
                return 'MIXED'
            return 'STANDARD'

        except Exception as e:
            self.loggit.warning(f"Failed to check S3 storage for {bucket}/{base_path}: {e}")
            return 'ERROR'

    def do_dry_run(self) -> None:
        """
        Perform dry-run showing what would be repaired without making changes.
        """
        self.loggit.info("Starting RepairMetadata dry-run...")
        self._perform_repair(dry_run=True)

    def do_action(self) -> None:
        """
        Perform the metadata repair operation.
        """
        self.loggit.info("Starting RepairMetadata action...")
        self._perform_repair(dry_run=False)

    def _perform_repair(self, dry_run: bool = False) -> None:
        """
        Core logic to scan repositories and fix metadata discrepancies.

        :param dry_run: If True, show what would be changed without making modifications
        :type dry_run: bool
        """
        # Get all repositories from status index
        query = {
            'query': {'term': {'doctype': 'repository'}},
            'size': 1000,
            'sort': [{'start': 'asc'}]
        }

        try:
            response = self.client.search(index=STATUS_INDEX, body=query)
        except Exception as e:
            self.loggit.error(f"Failed to query status index: {e}")
            return

        repos_data = []
        for hit in response['hits']['hits']:
            source = hit['_source']
            repos_data.append({
                'name': source['name'],
                'bucket': source.get('bucket'),
                'base_path': source.get('base_path'),
                'thaw_state': source.get('thaw_state', 'unknown'),
                'is_mounted': source.get('is_mounted', False),
            })

        if not repos_data:
            self.loggit.info("No repositories found in status index")
            if not self.porcelain:
                console = Console()
                console.print("[yellow]No repositories found[/yellow]")
            return

        self.loggit.info(f"Found {len(repos_data)} repositories to check")

        # Scan each repository and collect discrepancies
        discrepancies = []
        correct = []
        errors = []

        for repo_data in repos_data:
            name = repo_data['name']
            bucket = repo_data['bucket']
            base_path = repo_data['base_path']
            thaw_state = repo_data['thaw_state']

            if not bucket or not base_path:
                self.loggit.debug(f"Skipping {name} - no bucket/base_path info")
                continue

            # Check actual S3 storage class
            actual_storage = self._check_repo_storage_class(bucket, base_path)

            if actual_storage == 'ERROR':
                errors.append({
                    'name': name,
                    'error': 'Failed to check S3'
                })
                continue

            # Determine if metadata is correct
            expected_frozen = (thaw_state == 'frozen')
            actually_frozen = (actual_storage == 'GLACIER')

            if expected_frozen != actually_frozen:
                discrepancies.append({
                    'name': name,
                    'metadata_state': thaw_state,
                    'actual_storage': actual_storage,
                    'mounted': repo_data['is_mounted']
                })
            else:
                correct.append({
                    'name': name,
                    'state': thaw_state,
                    'storage': actual_storage
                })

        # Report results
        if self.porcelain:
            # Plain text output for scripting
            print(f"TOTAL_REPOS={len(repos_data)}")
            print(f"CORRECT={len(correct)}")
            print(f"DISCREPANCIES={len(discrepancies)}")
            print(f"ERRORS={len(errors)}")
            if discrepancies:
                print("REPOS_TO_FIX:")
                for d in discrepancies:
                    print(f"  {d['name']}: metadata={d['metadata_state']}, actual={d['actual_storage']}")
        else:
            # Rich formatted output
            console = Console()
            console.print(f"\n[bold]Metadata Repair Report ({'DRY-RUN' if dry_run else 'LIVE'})[/bold]\n")

            # Summary
            console.print(f"Total repositories scanned: [cyan]{len(repos_data)}[/cyan]")
            console.print(f"Repositories with correct metadata: [green]{len(correct)}[/green]")
            console.print(f"Repositories with discrepancies: [yellow]{len(discrepancies)}[/yellow]")
            if errors:
                console.print(f"Repositories with errors: [red]{len(errors)}[/red]")

            # Show discrepancies
            if discrepancies:
                console.print("\n[bold yellow]Discrepancies Found:[/bold yellow]")
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Repository", style="cyan")
                table.add_column("Metadata State", style="yellow")
                table.add_column("Actual S3 Storage", style="green")
                table.add_column("Mounted", style="dim")

                for d in discrepancies:
                    table.add_row(
                        d['name'],
                        d['metadata_state'],
                        d['actual_storage'],
                        "Yes" if d['mounted'] else "No"
                    )

                console.print(table)

            # Show errors
            if errors:
                console.print("\n[bold red]Errors:[/bold red]")
                for e in errors:
                    console.print(f"  [red]{e['name']}[/red]: {e['error']}")

        # Fix discrepancies if not dry-run
        if not dry_run and discrepancies:
            self.loggit.info(f"Fixing {len(discrepancies)} repositories...")

            fixed_count = 0
            failed_count = 0

            for d in discrepancies:
                repo_name = d['name']
                actual_storage = d['actual_storage']

                try:
                    # Fetch the repository object
                    repos = get_repositories_by_names(self.client, [repo_name])
                    if not repos:
                        self.loggit.error(f"Repository {repo_name} not found")
                        failed_count += 1
                        continue

                    repo = repos[0]

                    # Update state based on actual storage
                    if actual_storage == 'GLACIER':
                        self.loggit.info(f"Setting {repo_name} to frozen state")
                        repo.reset_to_frozen()
                        repo.persist(self.client)
                        fixed_count += 1
                    elif actual_storage == 'STANDARD':
                        # Only update if currently marked as frozen
                        if d['metadata_state'] == 'frozen':
                            self.loggit.info(f"Setting {repo_name} to active state (S3 is STANDARD)")
                            repo.thaw_state = 'active'
                            repo.is_thawed = False
                            repo.persist(self.client)
                            fixed_count += 1
                    else:
                        self.loggit.warning(f"Skipping {repo_name} with MIXED or EMPTY storage")
                        failed_count += 1

                except Exception as e:
                    self.loggit.error(f"Failed to fix {repo_name}: {e}")
                    failed_count += 1

            if not self.porcelain:
                console.print(f"\n[bold]Results:[/bold]")
                console.print(f"  Fixed: [green]{fixed_count}[/green]")
                if failed_count > 0:
                    console.print(f"  Failed: [red]{failed_count}[/red]")
            else:
                print(f"FIXED={fixed_count}")
                print(f"FAILED={failed_count}")

            self.loggit.info(f"Repair complete: {fixed_count} fixed, {failed_count} failed")

        elif dry_run and discrepancies:
            if not self.porcelain:
                console.print("\n[yellow]DRY-RUN: No changes made. Run without --dry-run to apply fixes.[/yellow]")

    def do_singleton_action(self, dry_run: bool = False) -> None:
        """
        Entry point for singleton CLI execution.

        :param dry_run: If True, show what would be changed without making modifications
        :type dry_run: bool
        """
        if dry_run:
            self.do_dry_run()
        else:
            self.do_action()
