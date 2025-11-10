"""Repair Metadata action for deepfreeze"""

# pylint: disable=too-many-instance-attributes

import logging

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.table import Table

from curator.actions.deepfreeze import STATUS_INDEX
from curator.actions.deepfreeze.utilities import (
    check_restore_status,
    get_repositories_by_names,
    get_settings,
    list_thaw_requests,
    update_thaw_request,
)
from curator.s3client import s3_client_factory


class RepairMetadata:
    """
    The RepairMetadata action scans all repositories and thaw requests and fixes metadata
    discrepancies where the status index shows incorrect state compared to actual S3 state.

    This action:
    1. Scans all repositories in the status index
    2. Checks actual S3 storage class for each repository
    3. Updates thaw_state='frozen' for repositories actually stored in GLACIER
    4. Scans all in_progress thaw requests
    5. Checks actual S3 restore status for each thaw request
    6. Updates thaw request status based on actual restore state:
       - 'refrozen' if all restores have expired
       - 'completed' if all restores are complete (but doesn't mount - use thaw --check-status for that)
    7. Reports on all changes made

    This is useful when metadata gets out of sync with reality, such as when:
    - Rotation pushes repos to GLACIER but doesn't update thaw_state
    - Manual S3 operations change storage class
    - Errors during state transitions
    - Thaw requests created but never checked, causing stale in_progress status

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

    def _check_thaw_request_restore_status(self, request: dict) -> dict:
        """
        Check the actual S3 restore status for all repositories in a thaw request.

        :param request: Thaw request document
        :type request: dict

        :return: Status dict with 'actual_state', 'all_expired', 'all_complete', 'in_progress'
        :rtype: dict
        """
        request_id = request.get('id', 'unknown')
        repo_names = request.get('repos', [])

        if not repo_names:
            self.loggit.warning(f"Thaw request {request_id} has no repos")
            return {
                'actual_state': 'UNKNOWN',
                'all_expired': False,
                'all_complete': False,
                'in_progress': False,
            }

        # Get repository objects
        try:
            repos = get_repositories_by_names(self.client, repo_names)
        except Exception as e:
            self.loggit.warning(f"Failed to get repositories for request {request_id}: {e}")
            return {
                'actual_state': 'ERROR',
                'all_expired': False,
                'all_complete': False,
                'in_progress': False,
            }

        if not repos:
            self.loggit.warning(f"No repositories found for request {request_id}")
            return {
                'actual_state': 'NO_REPOS',
                'all_expired': False,
                'all_complete': False,
                'in_progress': False,
            }

        # Check restore status for each repository
        all_complete = True
        all_expired = True
        any_in_progress = False

        for repo in repos:
            try:
                status = check_restore_status(self.s3_wrapper, repo.bucket, repo.base_path)

                if status.get('in_progress', 0) > 0:
                    any_in_progress = True
                    all_complete = False
                    all_expired = False

                if not status.get('complete', False):
                    all_complete = False

                # If any objects are restored or in progress, not all are expired
                if status.get('restored', 0) > 0 or status.get('in_progress', 0) > 0:
                    all_expired = False

            except Exception as e:
                self.loggit.warning(f"Failed to check restore status for {repo.name}: {e}")
                all_complete = False
                all_expired = False

        # Determine actual state
        if all_complete:
            actual_state = 'COMPLETED'
        elif any_in_progress:
            actual_state = 'IN_PROGRESS'
        elif all_expired:
            actual_state = 'EXPIRED'
        else:
            actual_state = 'MIXED'

        return {
            'actual_state': actual_state,
            'all_expired': all_expired,
            'all_complete': all_complete,
            'in_progress': any_in_progress,
        }

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

        # Check thaw requests for stale metadata
        self.loggit.info("Checking thaw requests for stale metadata")
        stale_thaw_requests = []
        correct_thaw_requests = []
        thaw_request_errors = []

        try:
            all_thaw_requests = list_thaw_requests(self.client)
            # Only check in_progress requests (others are terminal states or already handled)
            in_progress_requests = [req for req in all_thaw_requests if req.get('status') == 'in_progress']

            self.loggit.info(f"Found {len(in_progress_requests)} in_progress thaw requests to check")

            for request in in_progress_requests:
                request_id = request.get('id', 'unknown')
                try:
                    restore_status = self._check_thaw_request_restore_status(request)
                    actual_state = restore_status['actual_state']
                    metadata_state = request.get('status', 'unknown')

                    # Check if metadata is stale
                    if actual_state == 'EXPIRED':
                        # All restores expired, but metadata still says in_progress
                        stale_thaw_requests.append({
                            'id': request_id,
                            'repos': request.get('repos', []),
                            'metadata_state': metadata_state,
                            'actual_state': actual_state,
                            'created_at': request.get('created_at'),
                            'should_be': 'refrozen',
                        })
                    elif actual_state == 'COMPLETED':
                        # Restore complete, but metadata still says in_progress
                        stale_thaw_requests.append({
                            'id': request_id,
                            'repos': request.get('repos', []),
                            'metadata_state': metadata_state,
                            'actual_state': actual_state,
                            'created_at': request.get('created_at'),
                            'should_be': 'completed',
                        })
                    elif actual_state == 'IN_PROGRESS':
                        # Correctly marked as in_progress
                        correct_thaw_requests.append({
                            'id': request_id,
                            'state': metadata_state,
                        })
                    elif actual_state in ['ERROR', 'UNKNOWN', 'NO_REPOS', 'MIXED']:
                        thaw_request_errors.append({
                            'id': request_id,
                            'error': f'Unable to determine state: {actual_state}',
                        })
                    else:
                        # Might be in mixed state - keep as is for safety
                        correct_thaw_requests.append({
                            'id': request_id,
                            'state': metadata_state,
                        })

                except Exception as e:
                    self.loggit.error(f"Error checking thaw request {request_id}: {e}")
                    thaw_request_errors.append({
                        'id': request_id,
                        'error': str(e),
                    })

        except Exception as e:
            self.loggit.error(f"Failed to check thaw requests: {e}")

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
            print(f"TOTAL_THAW_REQUESTS={len(in_progress_requests)}")
            print(f"CORRECT_THAW_REQUESTS={len(correct_thaw_requests)}")
            print(f"STALE_THAW_REQUESTS={len(stale_thaw_requests)}")
            print(f"THAW_REQUEST_ERRORS={len(thaw_request_errors)}")
            if stale_thaw_requests:
                print("THAW_REQUESTS_TO_FIX:")
                for req in stale_thaw_requests:
                    print(f"  {req['id']}: metadata={req['metadata_state']}, actual={req['actual_state']}, should_be={req['should_be']}")
        else:
            # Rich formatted output
            console = Console()
            console.print(f"\n[bold]Metadata Repair Report ({'DRY-RUN' if dry_run else 'LIVE'})[/bold]\n")

            # Repository summary
            console.print("[bold cyan]REPOSITORIES:[/bold cyan]")
            console.print(f"  Total scanned: [cyan]{len(repos_data)}[/cyan]")
            console.print(f"  Correct metadata: [green]{len(correct)}[/green]")
            console.print(f"  Discrepancies: [yellow]{len(discrepancies)}[/yellow]")
            if errors:
                console.print(f"  Errors: [red]{len(errors)}[/red]")

            # Thaw request summary
            console.print(f"\n[bold cyan]THAW REQUESTS:[/bold cyan]")
            console.print(f"  Total in_progress: [cyan]{len(in_progress_requests)}[/cyan]")
            console.print(f"  Correct metadata: [green]{len(correct_thaw_requests)}[/green]")
            console.print(f"  Stale metadata: [yellow]{len(stale_thaw_requests)}[/yellow]")
            if thaw_request_errors:
                console.print(f"  Errors: [red]{len(thaw_request_errors)}[/red]")

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
                console.print("\n[bold red]Repository Errors:[/bold red]")
                for e in errors:
                    console.print(f"  [red]{e['name']}[/red]: {e['error']}")

            # Show stale thaw requests
            if stale_thaw_requests:
                console.print("\n[bold yellow]Stale Thaw Requests Found:[/bold yellow]")
                thaw_table = Table(show_header=True, header_style="bold magenta")
                thaw_table.add_column("Request ID", style="cyan")
                thaw_table.add_column("Repositories", style="dim")
                thaw_table.add_column("Metadata State", style="yellow")
                thaw_table.add_column("Actual State", style="green")
                thaw_table.add_column("Should Be", style="blue")
                thaw_table.add_column("Created", style="dim")

                for req in stale_thaw_requests:
                    repos_str = ", ".join(req['repos'][:3])  # Show first 3 repos
                    if len(req['repos']) > 3:
                        repos_str += f" (+{len(req['repos'])-3} more)"

                    created_at = req.get('created_at', 'unknown')
                    if created_at != 'unknown':
                        # Shorten ISO timestamp for display
                        created_at = created_at[:10]  # Just the date part

                    thaw_table.add_row(
                        req['id'][:8] + "...",  # Shorten UUID for display
                        repos_str,
                        req['metadata_state'],
                        req['actual_state'],
                        req['should_be'],
                        created_at
                    )

                console.print(thaw_table)

            # Show thaw request errors
            if thaw_request_errors:
                console.print("\n[bold red]Thaw Request Errors:[/bold red]")
                for e in thaw_request_errors:
                    console.print(f"  [red]{e['id'][:8]}...[/red]: {e['error']}")

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

            self.loggit.info(f"Repository repair complete: {fixed_count} fixed, {failed_count} failed")

        # Fix stale thaw requests if not dry-run
        if not dry_run and stale_thaw_requests:
            self.loggit.info(f"Fixing {len(stale_thaw_requests)} stale thaw requests...")

            thaw_fixed_count = 0
            thaw_failed_count = 0

            for req in stale_thaw_requests:
                request_id = req['id']
                should_be = req['should_be']

                try:
                    if should_be == 'refrozen':
                        # Mark as refrozen since restore expired
                        self.loggit.info(f"Marking thaw request {request_id} as refrozen (restore expired)")
                        update_thaw_request(self.client, request_id, status='refrozen')
                        thaw_fixed_count += 1

                    elif should_be == 'completed':
                        # Mark as completed since restore is done
                        # Note: This doesn't mount the repos - user should use thaw --check-status for that
                        self.loggit.info(f"Marking thaw request {request_id} as completed (restore finished)")
                        update_thaw_request(self.client, request_id, status='completed')
                        self.loggit.warning(
                            f"Thaw request {request_id} marked completed but repositories NOT mounted. "
                            f"Run 'curator_cli deepfreeze thaw --check-status {request_id}' to mount."
                        )
                        thaw_fixed_count += 1

                except Exception as e:
                    self.loggit.error(f"Failed to fix thaw request {request_id}: {e}")
                    thaw_failed_count += 1

            if not self.porcelain:
                console.print(f"\n[bold]Thaw Request Repair Results:[/bold]")
                console.print(f"  Fixed: [green]{thaw_fixed_count}[/green]")
                if thaw_failed_count > 0:
                    console.print(f"  Failed: [red]{thaw_failed_count}[/red]")
            else:
                print(f"THAW_FIXED={thaw_fixed_count}")
                print(f"THAW_FAILED={thaw_failed_count}")

            self.loggit.info(f"Thaw request repair complete: {thaw_fixed_count} fixed, {thaw_failed_count} failed")

        elif dry_run and (discrepancies or stale_thaw_requests):
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
