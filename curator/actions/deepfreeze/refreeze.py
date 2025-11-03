"""Refreeze action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch8 import Elasticsearch, NotFoundError
from rich import print as rprint

from curator.actions.deepfreeze.constants import STATUS_INDEX, THAW_STATUS_REFROZEN
from curator.actions.deepfreeze.exceptions import MissingIndexError
from curator.actions.deepfreeze.utilities import (
    get_all_indices_in_repo,
    get_repositories_by_names,
    get_settings,
    get_thaw_request,
    list_thaw_requests,
)
from curator.exceptions import ActionError


class Refreeze:
    """
    The Refreeze action is a user-initiated operation to signal "I'm done with this thaw."
    It unmounts repositories that were previously thawed and resets them back to frozen state.

    Unlike the automatic Cleanup action which processes expired repositories on a schedule,
    Refreeze is explicitly invoked by users when they're finished accessing thawed data,
    even if the S3 restore hasn't expired yet.

    When you thaw from AWS Glacier, you get a temporary restored copy that exists for a
    specified duration. After that expires, AWS automatically removes the temporary copy -
    the original Glacier object never moved. Refreeze doesn't push anything back; it's about
    unmounting the repositories and resetting state.

    :param client: A client connection object
    :type client: Elasticsearch
    :param thaw_request_id: The ID of the thaw request to refreeze (optional - if None, all completed requests)
    :type thaw_request_id: str

    :methods:
        do_action: Perform the refreeze operation.
        do_dry_run: Perform a dry-run of the refreeze operation.
        do_singleton_action: Entry point for singleton CLI execution.
    """

    def __init__(
        self,
        client: Elasticsearch,
        thaw_request_id: str = None,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Refreeze")

        self.client = client
        self.thaw_request_id = thaw_request_id
        self.porcelain = porcelain

        # CRITICAL FIX: Validate that settings exist before proceeding
        try:
            self.settings = get_settings(client)
            if self.settings is None:
                error_msg = (
                    f"Deepfreeze settings not found in status index {STATUS_INDEX}. "
                    f"Run 'curator_cli deepfreeze setup' first to initialize deepfreeze."
                )
                self.loggit.error(error_msg)
                if self.porcelain:
                    rprint(f"ERROR\tmissing_settings\t{error_msg}")
                raise ActionError(error_msg)
            self.loggit.debug("Settings loaded successfully")
        except MissingIndexError as e:
            error_msg = f"Status index {STATUS_INDEX} does not exist. Run 'curator_cli deepfreeze setup' first."
            self.loggit.error(error_msg)
            if self.porcelain:
                rprint(f"ERROR\tmissing_index\t{error_msg}")
            raise ActionError(error_msg) from e

        if thaw_request_id:
            self.loggit.info(
                "Deepfreeze Refreeze initialized for request %s", thaw_request_id
            )
        else:
            self.loggit.info(
                "Deepfreeze Refreeze initialized for all completed thaw requests"
            )

    def _get_open_thaw_requests(self) -> list:
        """
        Get all completed thaw requests that are eligible for refreezing.

        Returns thaw requests with status "completed" - these are requests where
        restoration finished and repositories are mounted, making them ready to be
        refrozen when the user is done accessing the data.

        :return: List of thaw request dicts
        :rtype: list
        """
        all_requests = list_thaw_requests(self.client)
        return [req for req in all_requests if req.get("status") == "completed"]

    def _confirm_bulk_refreeze(self, requests: list) -> bool:
        """
        Display a list of thaw requests and get user confirmation to proceed.
        In porcelain mode, automatically returns True (no interactive confirmation).

        :param requests: List of thaw request dicts
        :type requests: list

        :return: True if user confirms (or in porcelain mode), False otherwise
        :rtype: bool
        """
        # In porcelain mode, skip confirmation and just proceed
        if self.porcelain:
            return True

        rprint(
            f"\n[bold yellow]WARNING: This will refreeze {len(requests)} completed thaw request(s)[/bold yellow]\n"
        )

        # Show the requests
        for req in requests:
            request_id = req.get("id")
            repo_count = len(req.get("repos", []))
            created_at = req.get("created_at", "Unknown")
            start_date = req.get("start_date", "--")
            end_date = req.get("end_date", "--")

            rprint(f"  [cyan]• {request_id}[/cyan]")
            rprint(f"    [dim]Created: {created_at}[/dim]")
            rprint(f"    [dim]Date Range: {start_date} to {end_date}[/dim]")
            rprint(f"    [dim]Repositories: {repo_count}[/dim]\n")

        # Get confirmation
        try:
            response = (
                input(
                    "Do you want to proceed with refreezing all these requests? [y/N]: "
                )
                .strip()
                .lower()
            )
            return response in ['y', 'yes']
        except (EOFError, KeyboardInterrupt):
            rprint("\n[yellow]Operation cancelled by user[/yellow]")
            return False

    def _delete_mounted_indices_for_repo(self, repo_name: str) -> tuple[int, list[str]]:
        """
        Delete all mounted indices from a repository.

        Searchable snapshot indices can exist with multiple name variations:
        - Original name (e.g., .ds-df-test-2024.01.01-000001)
        - partial- prefix (e.g., partial-.ds-df-test-2024.01.01-000001)
        - restored- prefix (e.g., restored-.ds-df-test-2024.01.01-000001)

        :param repo_name: The repository name
        :type repo_name: str

        :return: Tuple of (deleted_count, failed_indices)
        :rtype: tuple[int, list[str]]
        """
        deleted_count = 0
        failed_indices = []

        try:
            # Get all indices from repository snapshots
            snapshot_indices = get_all_indices_in_repo(self.client, repo_name)
            self.loggit.debug(
                "Found %d indices in repository %s snapshots",
                len(snapshot_indices),
                repo_name,
            )

            # Check for each index with all possible name variations
            for base_index in snapshot_indices:
                # Try all possible index name variations
                possible_names = [
                    base_index,  # Original name
                    f"partial-{base_index}",  # Searchable snapshot
                    f"restored-{base_index}",  # Fully restored
                ]

                for index_name in possible_names:
                    try:
                        if self.client.indices.exists(index=index_name):
                            self.loggit.info(
                                "Deleting index %s from repository %s",
                                index_name,
                                repo_name,
                            )
                            self.client.indices.delete(index=index_name)
                            deleted_count += 1
                            self.loggit.debug(
                                "Successfully deleted index %s", index_name
                            )
                            # Only try one variation - if we found and deleted it, stop
                            break
                    except Exception as e:
                        self.loggit.error(
                            "Failed to delete index %s: %s (type: %s)",
                            index_name,
                            e,
                            type(e).__name__,
                            exc_info=True,
                        )
                        failed_indices.append(index_name)

        except NotFoundError as e:
            # Repository has already been unmounted - this is expected during refreeze
            if "repository_missing_exception" in str(e).lower():
                self.loggit.info(
                    "Repository %s has already been unmounted, no indices to delete",
                    repo_name,
                )
            else:
                self.loggit.warning(
                    "Repository %s not found: %s",
                    repo_name,
                    e,
                )
            return 0, []
        except Exception as e:
            self.loggit.error(
                "Failed to get indices from repository %s: %s",
                repo_name,
                e,
                exc_info=True,
            )
            return 0, []

        return deleted_count, failed_indices

    def _delete_thawed_ilm_policy(self, repo_name: str) -> bool:
        """
        Delete the per-repository thawed ILM policy.

        Policy name format: {repo_name}-thawed (e.g., deepfreeze-000010-thawed)

        Before deleting the policy, removes it from any indices still using it to avoid
        "policy in use" errors.

        :param repo_name: The repository name
        :type repo_name: str

        :return: True if deleted successfully, False otherwise
        :rtype: bool
        """
        policy_name = f"{repo_name}-thawed"

        try:
            # Check if policy exists first
            self.client.ilm.get_lifecycle(name=policy_name)

            # Before deleting, remove the policy from any indices still using it
            self.loggit.debug("Checking for indices using ILM policy %s", policy_name)
            try:
                # Get all indices using this policy
                ilm_explain = self.client.ilm.explain_lifecycle(index="*")
                indices_using_policy = [
                    idx
                    for idx, info in ilm_explain.get("indices", {}).items()
                    if info.get("policy") == policy_name
                ]

                if indices_using_policy:
                    self.loggit.info(
                        "Found %d indices still using policy %s, removing policy from them",
                        len(indices_using_policy),
                        policy_name,
                    )
                    for idx in indices_using_policy:
                        try:
                            self.loggit.debug("Removing ILM policy from index %s", idx)
                            self.client.ilm.remove_policy(index=idx)
                        except Exception as idx_err:
                            self.loggit.warning(
                                "Failed to remove ILM policy from index %s: %s",
                                idx,
                                idx_err,
                            )
            except Exception as check_err:
                self.loggit.warning(
                    "Failed to check for indices using policy %s: %s",
                    policy_name,
                    check_err,
                )

            # Policy exists and indices have been cleaned up, delete it
            self.loggit.info("Deleting thawed ILM policy %s", policy_name)
            self.client.ilm.delete_lifecycle(name=policy_name)
            self.loggit.debug("Successfully deleted ILM policy %s", policy_name)
            return True

        except Exception as e:
            # If policy doesn't exist (404), that's okay - might be pre-ILM implementation
            if "404" in str(e) or "resource_not_found" in str(e).lower():
                self.loggit.debug(
                    "ILM policy %s does not exist, skipping deletion", policy_name
                )
                return True
            else:
                self.loggit.warning(
                    "Failed to delete ILM policy %s: %s (type: %s)",
                    policy_name,
                    e,
                    type(e).__name__,
                    exc_info=True,
                )
                return False

    def _refreeze_single_request(self, request_id: str) -> dict:
        """
        Refreeze a single thaw request.

        Operations performed for each repository:
        1. Delete all mounted indices from the repository
        2. Unmount the repository from Elasticsearch
        3. Delete the per-repository thawed ILM policy
        4. Reset repository state to frozen
        5. Persist state changes

        :param request_id: The thaw request ID
        :type request_id: str

        :return: Dict with unmounted_repos, failed_repos, deleted_indices, deleted_policies
        :rtype: dict
        """
        self.loggit.info("Refreezing thaw request %s", request_id)

        # Get the thaw request
        try:
            request = get_thaw_request(self.client, request_id)
        except Exception as e:
            self.loggit.error("Failed to get thaw request %s: %s", request_id, e)
            if self.porcelain:
                print(f"ERROR\trequest_not_found\t{request_id}\t{str(e)}")
            else:
                rprint(f"[red]Error: Could not find thaw request '{request_id}'[/red]")
            return {
                "unmounted_repos": [],
                "failed_repos": [],
                "deleted_indices": 0,
                "deleted_policies": 0,
            }

        # Get the repositories from the request
        repo_names = request.get("repos", [])
        if not repo_names:
            self.loggit.warning("No repositories found in thaw request %s", request_id)
            return {
                "unmounted_repos": [],
                "failed_repos": [],
                "deleted_indices": 0,
                "deleted_policies": 0,
            }

        self.loggit.info("Found %d repositories to refreeze", len(repo_names))

        # Get the repository objects
        try:
            repos = get_repositories_by_names(self.client, repo_names)
        except Exception as e:
            self.loggit.error("Failed to get repositories: %s", e)
            return {
                "unmounted_repos": [],
                "failed_repos": [],
                "deleted_indices": 0,
                "deleted_policies": 0,
            }

        if not repos:
            self.loggit.warning("No repository objects found for names: %s", repo_names)
            return {
                "unmounted_repos": [],
                "failed_repos": [],
                "deleted_indices": 0,
                "deleted_policies": 0,
            }

        # Track success/failure and statistics
        unmounted = []
        failed = []
        total_deleted_indices = 0
        total_deleted_policies = 0

        # Process each repository
        for repo in repos:
            # ENHANCED LOGGING: Add detailed repository state information
            self.loggit.info(
                "Processing repository %s - current state: mounted=%s, thaw_state=%s, bucket=%s, base_path=%s",
                repo.name,
                repo.is_mounted,
                repo.thaw_state,
                repo.bucket,
                repo.base_path,
            )

            try:
                # STEP 1: Delete mounted indices BEFORE unmounting repository
                self.loggit.info(
                    "Deleting mounted indices for repository %s", repo.name
                )
                deleted_count, failed_indices = self._delete_mounted_indices_for_repo(
                    repo.name
                )
                total_deleted_indices += deleted_count
                if deleted_count > 0:
                    self.loggit.info(
                        "Deleted %d indices from repository %s",
                        deleted_count,
                        repo.name,
                    )
                if failed_indices:
                    self.loggit.warning(
                        "Failed to delete %d indices from repository %s",
                        len(failed_indices),
                        repo.name,
                    )

                # STEP 2: Unmount repository if still mounted
                if repo.is_mounted:
                    try:
                        self.loggit.info("Unmounting repository %s", repo.name)
                        self.client.snapshot.delete_repository(name=repo.name)
                        self.loggit.info(
                            "Successfully unmounted repository %s", repo.name
                        )
                        unmounted.append(repo.name)
                    except Exception as e:
                        # If it's already unmounted, that's okay
                        if "repository_missing_exception" in str(e).lower():
                            self.loggit.debug(
                                "Repository %s was already unmounted", repo.name
                            )
                        else:
                            self.loggit.warning(
                                "Failed to unmount repository %s: %s (type: %s)",
                                repo.name,
                                e,
                                type(e).__name__,
                                exc_info=True,
                            )
                            # Continue anyway to update the state
                else:
                    self.loggit.debug(
                        "Repository %s was not mounted, skipping unmount", repo.name
                    )

                # STEP 3: Delete per-repository thawed ILM policy
                if self._delete_thawed_ilm_policy(repo.name):
                    total_deleted_policies += 1
                    self.loggit.debug("Deleted ILM policy for repository %s", repo.name)

                # STEP 4: Reset to frozen state
                self.loggit.debug(
                    "Resetting repository %s to frozen state (old state: %s)",
                    repo.name,
                    repo.thaw_state,
                )
                repo.reset_to_frozen()

                # Persist the state change
                self.loggit.debug(
                    "Persisting state change for repository %s", repo.name
                )
                repo.persist(self.client)
                self.loggit.info(
                    "Repository %s successfully reset to frozen state and persisted",
                    repo.name,
                )

            except Exception as e:
                self.loggit.error(
                    "Error processing repository %s: %s (type: %s)",
                    repo.name,
                    e,
                    type(e).__name__,
                    exc_info=True,
                )
                failed.append(repo.name)

        # STEP 5: Update the thaw request status to refrozen
        # (Cleanup action will remove old refrozen requests based on retention settings)
        try:
            self.client.update(
                index=STATUS_INDEX,
                id=request_id,
                body={"doc": {"status": THAW_STATUS_REFROZEN}},
            )
            self.loggit.info("Thaw request %s marked as refrozen", request_id)
        except Exception as e:
            self.loggit.error("Failed to update thaw request status: %s", e)

        return {
            "unmounted_repos": unmounted,
            "failed_repos": failed,
            "deleted_indices": total_deleted_indices,
            "deleted_policies": total_deleted_policies,
        }

    def do_action(self) -> None:
        """
        Unmount repositories from thaw request(s) and reset them to frozen state.

        If thaw_request_id is provided, refreeze that specific request.
        If thaw_request_id is None, refreeze all completed requests (with confirmation).

        :return: None
        :rtype: None
        """
        # Determine which requests to process
        if self.thaw_request_id:
            # Single request mode
            request_ids = [self.thaw_request_id]
        else:
            # Bulk mode - get all open requests
            open_requests = self._get_open_thaw_requests()

            if not open_requests:
                if not self.porcelain:
                    rprint(
                        "[yellow]No completed thaw requests found to refreeze[/yellow]"
                    )
                return

            # Get confirmation
            if not self._confirm_bulk_refreeze(open_requests):
                if not self.porcelain:
                    rprint("[yellow]Refreeze operation cancelled[/yellow]")
                return

            request_ids = [req.get("id") for req in open_requests]

        # Process each request
        total_unmounted = []
        total_failed = []
        total_deleted_indices = 0
        total_deleted_policies = 0

        for request_id in request_ids:
            result = self._refreeze_single_request(request_id)
            total_unmounted.extend(result["unmounted_repos"])
            total_failed.extend(result["failed_repos"])
            total_deleted_indices += result["deleted_indices"]
            total_deleted_policies += result["deleted_policies"]

        # Report results
        if self.porcelain:
            # Machine-readable output: tab-separated values
            for repo_name in total_unmounted:
                print(f"UNMOUNTED\t{repo_name}")
            for repo_name in total_failed:
                print(f"FAILED\t{repo_name}")
            print(
                f"SUMMARY\t{len(total_unmounted)}\t{len(total_failed)}\t{total_deleted_indices}\t{total_deleted_policies}\t{len(request_ids)}"
            )
        else:
            if len(request_ids) == 1:
                rprint(
                    f"\n[green]Refreeze completed for thaw request '{request_ids[0]}'[/green]"
                )
            else:
                rprint(
                    f"\n[green]Refreeze completed for {len(request_ids)} thaw requests[/green]"
                )

            rprint(f"[cyan]Unmounted {len(total_unmounted)} repositories[/cyan]")
            rprint(f"[cyan]Deleted {total_deleted_indices} indices[/cyan]")
            rprint(f"[cyan]Deleted {total_deleted_policies} ILM policies[/cyan]")
            if total_failed:
                rprint(
                    f"[red]Failed to process {len(total_failed)} repositories: {', '.join(total_failed)}[/red]"
                )

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the refreeze operation.
        Shows which repositories would be unmounted and reset.

        If thaw_request_id is provided, show dry-run for that specific request.
        If thaw_request_id is None, show dry-run for all completed requests.

        :return: None
        :rtype: None
        """
        # Determine which requests to process
        if self.thaw_request_id:
            # Single request mode
            request_ids = [self.thaw_request_id]
            if not self.porcelain:
                rprint(
                    f"\n[cyan]DRY-RUN: Would refreeze thaw request '{self.thaw_request_id}'[/cyan]\n"
                )
        else:
            # Bulk mode - get all open requests
            open_requests = self._get_open_thaw_requests()

            if not open_requests:
                if not self.porcelain:
                    rprint(
                        "[yellow]DRY-RUN: No completed thaw requests found to refreeze[/yellow]"
                    )
                return

            if not self.porcelain:
                rprint(
                    f"\n[cyan]DRY-RUN: Would refreeze {len(open_requests)} completed thaw requests:[/cyan]\n"
                )

                # Show the requests
                for req in open_requests:
                    request_id = req.get("id")
                    repo_count = len(req.get("repos", []))
                    created_at = req.get("created_at", "Unknown")
                    start_date = req.get("start_date", "--")
                    end_date = req.get("end_date", "--")

                    rprint(f"  [cyan]• {request_id}[/cyan]")
                    rprint(f"    [dim]Created: {created_at}[/dim]")
                    rprint(f"    [dim]Date Range: {start_date} to {end_date}[/dim]")
                    rprint(f"    [dim]Repositories: {repo_count}[/dim]\n")

            request_ids = [req.get("id") for req in open_requests]

        # Process each request in dry-run mode
        total_repos = 0
        total_indices = 0
        total_policies = 0

        for request_id in request_ids:
            try:
                request = get_thaw_request(self.client, request_id)
            except Exception as e:
                self.loggit.error(
                    "DRY-RUN: Failed to get thaw request %s: %s", request_id, e
                )
                if self.porcelain:
                    print(f"ERROR\tdry_run_request_not_found\t{request_id}\t{str(e)}")
                else:
                    rprint(
                        f"[red]DRY-RUN: Could not find thaw request '{request_id}'[/red]"
                    )
                continue

            repo_names = request.get("repos", [])
            if not repo_names:
                continue

            try:
                repos = get_repositories_by_names(self.client, repo_names)
            except Exception as e:
                self.loggit.error(
                    "DRY-RUN: Failed to get repositories for request %s: %s",
                    request_id,
                    e,
                )
                continue

            if not repos:
                continue

            # Count indices and policies that would be deleted
            for repo in repos:
                # Count indices
                try:
                    snapshot_indices = get_all_indices_in_repo(self.client, repo.name)
                    for base_index in snapshot_indices:
                        # Check if any variation exists
                        possible_names = [
                            base_index,
                            f"partial-{base_index}",
                            f"restored-{base_index}",
                        ]
                        for index_name in possible_names:
                            if self.client.indices.exists(index=index_name):
                                total_indices += 1
                                break
                except Exception:
                    pass

                # Count policies
                policy_name = f"{repo.name}-thawed"
                try:
                    self.client.ilm.get_lifecycle(name=policy_name)
                    total_policies += 1
                except Exception:
                    pass

            # Show details if single request, or summary if bulk
            if self.porcelain:
                # Machine-readable output
                for repo in repos:
                    action = "unmount_and_reset" if repo.is_mounted else "reset"
                    # Count indices for this repo
                    repo_index_count = 0
                    try:
                        snapshot_indices = get_all_indices_in_repo(
                            self.client, repo.name
                        )
                        for base_index in snapshot_indices:
                            possible_names = [
                                base_index,
                                f"partial-{base_index}",
                                f"restored-{base_index}",
                            ]
                            for index_name in possible_names:
                                if self.client.indices.exists(index=index_name):
                                    repo_index_count += 1
                                    break
                    except Exception:
                        pass

                    # Check if policy exists
                    policy_exists = False
                    try:
                        self.client.ilm.get_lifecycle(name=f"{repo.name}-thawed")
                        policy_exists = True
                    except Exception:
                        pass

                    print(
                        f"DRY_RUN\t{repo.name}\t{repo.thaw_state}\t{repo.is_mounted}\t{action}\t{repo_index_count}\t{policy_exists}"
                    )
            else:
                if len(request_ids) == 1:
                    rprint(f"[cyan]Would process {len(repos)} repositories:[/cyan]\n")
                    for repo in repos:
                        action = (
                            "unmount and reset to frozen"
                            if repo.is_mounted
                            else "reset to frozen"
                        )
                        rprint(
                            f"  [cyan]- {repo.name}[/cyan] (state: {repo.thaw_state}, mounted: {repo.is_mounted})"
                        )
                        rprint(f"    [dim]Would {action}[/dim]")

                        # Show indices that would be deleted
                        try:
                            snapshot_indices = get_all_indices_in_repo(
                                self.client, repo.name
                            )
                            repo_index_count = 0
                            for base_index in snapshot_indices:
                                possible_names = [
                                    base_index,
                                    f"partial-{base_index}",
                                    f"restored-{base_index}",
                                ]
                                for index_name in possible_names:
                                    if self.client.indices.exists(index=index_name):
                                        repo_index_count += 1
                                        break
                            if repo_index_count > 0:
                                rprint(
                                    f"    [dim]Would delete {repo_index_count} mounted indices[/dim]"
                                )
                        except Exception:
                            pass

                        # Show ILM policy that would be deleted
                        policy_name = f"{repo.name}-thawed"
                        try:
                            self.client.ilm.get_lifecycle(name=policy_name)
                            rprint(
                                f"    [dim]Would delete ILM policy {policy_name}[/dim]"
                            )
                        except Exception:
                            pass

                    rprint(
                        f"\n[cyan]DRY-RUN: Would mark thaw request '{request_id}' as completed[/cyan]\n"
                    )

            total_repos += len(repos)

        # Summary for bulk mode
        if len(request_ids) > 1 and not self.porcelain:
            rprint(
                f"[cyan]DRY-RUN: Would process {total_repos} total repositories across {len(request_ids)} thaw requests[/cyan]"
            )
            rprint(
                f"[cyan]DRY-RUN: Would delete {total_indices} indices and {total_policies} ILM policies[/cyan]"
            )
            rprint(
                f"[cyan]DRY-RUN: Would mark {len(request_ids)} thaw requests as completed[/cyan]\n"
            )

        # Porcelain mode summary
        if self.porcelain:
            print(
                f"DRY_RUN_SUMMARY\t{total_repos}\t{total_indices}\t{total_policies}\t{len(request_ids)}"
            )

    def do_singleton_action(self) -> None:
        """
        Entry point for singleton CLI execution.

        :return: None
        :rtype: None
        """
        self.do_action()
