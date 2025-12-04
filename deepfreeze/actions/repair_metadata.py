"""RepairMetadata action for deepfreeze"""

# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging

from elasticsearch8 import Elasticsearch
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.markup import escape

from deepfreeze.constants import (
    STATUS_INDEX,
    THAW_STATE_ACTIVE,
    THAW_STATE_FROZEN,
    THAW_STATE_THAWED,
    THAW_STATE_THAWING,
)
from deepfreeze.exceptions import MissingIndexError, MissingSettingsError
from deepfreeze.s3client import s3_client_factory
from deepfreeze.utilities import (
    get_all_repos,
    get_settings,
)


class RepairMetadata:
    """
    RepairMetadata scans S3 storage classes and repairs any discrepancies
    between the actual S3 state and the status index.

    This is useful when:
    - S3 lifecycle policies have moved objects without deepfreeze knowing
    - Manual S3 operations have changed storage classes
    - Thaw operations completed but status wasn't updated

    :param client: A client connection object

    :methods:
        do_dry_run: Scan and report discrepancies without making changes
        do_action: Scan and repair discrepancies

    :example:
        >>> from deepfreeze.actions import RepairMetadata
        >>> repair = RepairMetadata(client)
        >>> repair.do_action()
    """

    def __init__(
        self,
        client: Elasticsearch,
        porcelain: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("deepfreeze.actions.repair_metadata")
        self.loggit.debug("Initializing Deepfreeze RepairMetadata")

        # Console for STDERR output
        self.console = Console(stderr=True)

        self.client = client
        self.porcelain = porcelain

        # Will be loaded during action
        self.settings = None
        self.s3 = None

        self.loggit.debug("Deepfreeze RepairMetadata initialized")

    def _load_settings(self) -> None:
        """Load settings from the status index."""
        self.loggit.debug("Loading settings")

        # Check if status index exists
        if not self.client.indices.exists(index=STATUS_INDEX):
            raise MissingIndexError(f"Status index {STATUS_INDEX} does not exist")

        self.settings = get_settings(self.client)
        if not self.settings:
            raise MissingSettingsError("Settings document not found in status index")

        # Initialize S3 client with provider from settings
        self.s3 = s3_client_factory(self.settings.provider)

    def _determine_actual_state(self, bucket: str, base_path: str) -> dict:
        """
        Determine the actual S3 storage state by checking object storage classes.

        :param bucket: The bucket name
        :param base_path: The base path in the bucket
        :return: Dictionary with storage class distribution and determined state
        """
        result = {
            "total_objects": 0,
            "storage_classes": {},
            "instant_access": 0,
            "glacier": 0,
            "restoring": 0,
            "determined_state": None,
        }

        try:
            # Normalize base_path
            normalized_path = base_path.strip("/")
            if normalized_path:
                normalized_path += "/"

            objects = self.s3.list_objects(bucket, normalized_path)
            result["total_objects"] = len(objects)

            for obj in objects:
                storage_class = obj.get("StorageClass", "STANDARD")

                # Count by storage class
                if storage_class not in result["storage_classes"]:
                    result["storage_classes"][storage_class] = 0
                result["storage_classes"][storage_class] += 1

                # Categorize
                if storage_class in [
                    "STANDARD",
                    "STANDARD_IA",
                    "ONEZONE_IA",
                    "INTELLIGENT_TIERING",
                ]:
                    result["instant_access"] += 1
                elif storage_class in ["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"]:
                    # Check if restoring
                    try:
                        metadata = self.s3.head_object(bucket, obj["Key"])
                        restore_header = metadata.get("Restore")
                        if restore_header and 'ongoing-request="true"' in restore_header:
                            result["restoring"] += 1
                        else:
                            result["glacier"] += 1
                    except Exception:
                        result["glacier"] += 1

            # Determine state based on distribution
            total = result["total_objects"]
            if total == 0:
                result["determined_state"] = THAW_STATE_ACTIVE
            elif result["restoring"] > 0:
                result["determined_state"] = THAW_STATE_THAWING
            elif result["glacier"] == total:
                result["determined_state"] = THAW_STATE_FROZEN
            elif result["instant_access"] == total:
                result["determined_state"] = THAW_STATE_THAWED
            elif result["instant_access"] > 0:
                # Mixed state - some instant access
                result["determined_state"] = THAW_STATE_THAWED
            else:
                result["determined_state"] = THAW_STATE_FROZEN

        except Exception as e:
            self.loggit.error(
                "Error checking storage state for %s/%s: %s", bucket, base_path, e
            )
            result["error"] = str(e)

        return result

    def _scan_repositories(self) -> list:
        """
        Scan all repositories and detect discrepancies.

        :return: List of discrepancy dictionaries
        """
        discrepancies = []

        repos = get_all_repos(self.client)

        for repo in repos:
            self.loggit.debug(
                "Checking repository %s (bucket: %s, base_path: %s)",
                repo.name,
                repo.bucket,
                repo.base_path,
            )

            # Get actual S3 state
            actual = self._determine_actual_state(repo.bucket, repo.base_path)

            if actual.get("error"):
                discrepancies.append(
                    {
                        "repo": repo.name,
                        "bucket": repo.bucket,
                        "base_path": repo.base_path,
                        "recorded_state": repo.thaw_state,
                        "actual_state": None,
                        "error": actual["error"],
                        "storage_classes": {},
                    }
                )
                continue

            # Compare recorded state vs actual state
            if repo.thaw_state != actual["determined_state"]:
                discrepancies.append(
                    {
                        "repo": repo.name,
                        "bucket": repo.bucket,
                        "base_path": repo.base_path,
                        "recorded_state": repo.thaw_state,
                        "actual_state": actual["determined_state"],
                        "storage_classes": actual["storage_classes"],
                        "total_objects": actual["total_objects"],
                        "instant_access": actual["instant_access"],
                        "glacier": actual["glacier"],
                        "restoring": actual["restoring"],
                    }
                )
                self.loggit.info(
                    "Discrepancy found for %s: recorded=%s, actual=%s",
                    repo.name,
                    repo.thaw_state,
                    actual["determined_state"],
                )

        return discrepancies

    def _repair_discrepancy(self, discrepancy: dict, dry_run: bool = False) -> dict:
        """
        Repair a single discrepancy by updating the status index.

        :param discrepancy: The discrepancy dictionary
        :param dry_run: If True, don't actually repair
        :return: Result dictionary
        """
        result = {
            "repo": discrepancy["repo"],
            "success": False,
            "old_state": discrepancy["recorded_state"],
            "new_state": discrepancy["actual_state"],
            "error": None,
        }

        if discrepancy.get("error"):
            result["error"] = f"Cannot repair: {discrepancy['error']}"
            return result

        if dry_run:
            result["success"] = True
            return result

        try:
            # Get the repo from status index and update
            from deepfreeze.helpers import Repository

            repo = Repository.from_elasticsearch(
                self.client, discrepancy["repo"], STATUS_INDEX
            )

            if not repo:
                result["error"] = "Repository not found in status index"
                return result

            # Update the thaw state
            repo.thaw_state = discrepancy["actual_state"]

            # Update is_thawed for backward compatibility
            if discrepancy["actual_state"] in [THAW_STATE_THAWED, THAW_STATE_THAWING]:
                repo.is_thawed = True
            else:
                repo.is_thawed = False

            # Update is_mounted based on actual S3 state
            if discrepancy["actual_state"] == THAW_STATE_FROZEN:
                repo.is_mounted = False

            # Persist changes
            repo.persist(self.client)
            result["success"] = True

            self.loggit.info(
                "Repaired %s: %s -> %s",
                discrepancy["repo"],
                discrepancy["recorded_state"],
                discrepancy["actual_state"],
            )

        except Exception as e:
            result["error"] = str(e)
            self.loggit.error("Failed to repair %s: %s", discrepancy["repo"], e)

        return result

    def do_dry_run(self) -> None:
        """
        Scan and report discrepancies without making changes.

        :return: None
        :rtype: None
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")

        try:
            self._load_settings()

            if not self.porcelain:
                self.console.print("[bold]Scanning repositories for discrepancies...[/bold]")

            discrepancies = self._scan_repositories()

            if self.porcelain:
                for d in discrepancies:
                    if d.get("error"):
                        print(
                            f"ERROR\t{d['repo']}\t{d.get('error')}"
                        )
                    else:
                        print(
                            f"DISCREPANCY\t{d['repo']}\t{d['recorded_state']}\t"
                            f"{d['actual_state']}\t{d.get('total_objects', 0)} objects"
                        )
                print(f"SUMMARY\t{len(discrepancies)} discrepancies found")
                return

            if not discrepancies:
                self.console.print(
                    Panel(
                        "[green]No discrepancies found![/green]\n\n"
                        "All repository states in the status index match the actual S3 storage classes.",
                        title="[bold green]Scan Complete[/bold green]",
                        border_style="green",
                        expand=False,
                    )
                )
                return

            # Display discrepancies in a table
            table = Table(title="Metadata Discrepancies")
            table.add_column("Repository", style="cyan")
            table.add_column("Recorded State", style="yellow")
            table.add_column("Actual State", style="green")
            table.add_column("Objects", style="white")
            table.add_column("Storage Classes", style="white")

            for d in discrepancies:
                if d.get("error"):
                    table.add_row(
                        d["repo"],
                        d["recorded_state"],
                        f"[red]Error: {d['error'][:30]}...[/red]",
                        "N/A",
                        "N/A",
                    )
                else:
                    storage_str = ", ".join(
                        [f"{k}: {v}" for k, v in d.get("storage_classes", {}).items()]
                    )
                    if len(storage_str) > 40:
                        storage_str = storage_str[:37] + "..."

                    table.add_row(
                        d["repo"],
                        d["recorded_state"],
                        d["actual_state"],
                        str(d.get("total_objects", 0)),
                        storage_str,
                    )

            self.console.print(table)
            self.console.print()

            self.console.print(
                Panel(
                    f"[bold]Found {len(discrepancies)} discrepancies[/bold]\n\n"
                    f"Run without [yellow]--dry-run[/yellow] to repair these discrepancies.",
                    title="[bold blue]Dry Run Summary[/bold blue]",
                    border_style="blue",
                    expand=False,
                )
            )

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(f"[red]Error: {e}[/red]")
            raise

    def do_action(self) -> None:
        """
        Scan and repair discrepancies.

        :return: None
        :rtype: None
        """
        self.loggit.debug("Starting RepairMetadata action")

        try:
            self._load_settings()

            if not self.porcelain:
                self.console.print("[bold]Scanning repositories for discrepancies...[/bold]")

            discrepancies = self._scan_repositories()

            if not discrepancies:
                if self.porcelain:
                    print("COMPLETE\t0 discrepancies")
                else:
                    self.console.print(
                        Panel(
                            "[green]No discrepancies found![/green]\n\n"
                            "All repository states in the status index match the actual S3 storage classes.",
                            title="[bold green]Scan Complete[/bold green]",
                            border_style="green",
                            expand=False,
                        )
                    )
                return

            if not self.porcelain:
                self.console.print(
                    f"[bold]Found {len(discrepancies)} discrepancies. Repairing...[/bold]"
                )

            # Repair each discrepancy
            results = []
            for d in discrepancies:
                result = self._repair_discrepancy(d)
                results.append(result)

            # Display results
            if self.porcelain:
                for r in results:
                    status = "SUCCESS" if r["success"] else "FAILED"
                    print(
                        f"{status}\t{r['repo']}\t{r['old_state']}\t{r['new_state']}"
                    )
                success_count = sum(1 for r in results if r["success"])
                fail_count = sum(1 for r in results if not r["success"])
                print(f"COMPLETE\t{success_count} repaired\t{fail_count} failed")
            else:
                # Summary
                success_count = sum(1 for r in results if r["success"])
                fail_count = sum(1 for r in results if not r["success"])

                if fail_count == 0:
                    self.console.print(
                        Panel(
                            f"[bold green]Successfully repaired {success_count} discrepancies![/bold green]",
                            title="[bold green]Repair Complete[/bold green]",
                            border_style="green",
                            expand=False,
                        )
                    )
                else:
                    # Show detailed results
                    table = Table(title="Repair Results")
                    table.add_column("Repository", style="cyan")
                    table.add_column("Old State", style="yellow")
                    table.add_column("New State", style="green")
                    table.add_column("Status", style="white")

                    for r in results:
                        if r["success"]:
                            status = "[green]Repaired[/green]"
                        else:
                            status = f"[red]Failed: {r.get('error', 'Unknown')}[/red]"

                        table.add_row(
                            r["repo"],
                            r.get("old_state", "N/A"),
                            r.get("new_state", "N/A"),
                            status,
                        )

                    self.console.print(table)
                    self.console.print()

                    self.console.print(
                        Panel(
                            f"[bold]Repair completed with some failures[/bold]\n\n"
                            f"Repaired: {success_count}\n"
                            f"Failed: {fail_count}\n\n"
                            f"Check logs for details on failures.",
                            title="[bold yellow]Repair Complete[/bold yellow]",
                            border_style="yellow",
                            expand=False,
                        )
                    )

            self.loggit.info(
                "RepairMetadata complete: %d repaired, %d failed",
                success_count,
                fail_count,
            )

        except (MissingIndexError, MissingSettingsError) as e:
            if self.porcelain:
                print(f"ERROR\t{type(e).__name__}\t{str(e)}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]Deepfreeze is not initialized[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Run [yellow]deepfreeze setup[/yellow] first.",
                        title="[bold red]Repair Failed[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            raise

        except Exception as e:
            if self.porcelain:
                print(f"ERROR\tunexpected\t{str(e)}")
            else:
                self.console.print(
                    Panel(
                        f"[bold]Repair operation failed[/bold]\n\n"
                        f"Error: {escape(str(e))}\n\n"
                        f"Check logs for details.",
                        title="[bold red]Repair Error[/bold red]",
                        border_style="red",
                        expand=False,
                    )
                )
            self.loggit.error("RepairMetadata failed: %s", e, exc_info=True)
            raise
