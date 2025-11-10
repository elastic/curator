# Thaw Request Tracking Implementation in Curator Deepfreeze

## Overview
This document details how thaw requests are currently tracked and managed in the deepfreeze functionality of Elasticsearch Curator, focusing on metadata storage, S3/Glacier API interactions, and status tracking mechanisms.

---

## 1. THAW REQUEST METADATA STORAGE

### Location: `curator/actions/deepfreeze/utilities.py`

#### save_thaw_request() - Lines 1040-1091
Stores a thaw request to the status index for later querying and status checking:

```python
def save_thaw_request(
    client: Elasticsearch,
    request_id: str,
    repos: list[Repository],
    status: str,
    start_date: datetime = None,
    end_date: datetime = None,
) -> None:
    """Save a thaw request to the status index for later querying."""
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Saving thaw request %s", request_id)

    request_doc = {
        "doctype": "thaw_request",
        "request_id": request_id,
        "repos": [repo.name for repo in repos],
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Add date range if provided
    if start_date:
        request_doc["start_date"] = start_date.isoformat()
    if end_date:
        request_doc["end_date"] = end_date.isoformat()

    try:
        client.index(index=STATUS_INDEX, id=request_id, body=request_doc)
        loggit.info("Thaw request %s saved successfully", request_id)
    except Exception as e:
        loggit.error("Failed to save thaw request %s: %s", request_id, e)
        raise ActionError(f"Failed to save thaw request {request_id}: {e}")
```

**Stored Fields:**
- `doctype`: "thaw_request"
- `request_id`: UUID for unique identification
- `repos`: List of repository names being thawed
- `status`: One of: "in_progress", "completed", "failed", "refrozen"
- `created_at`: ISO 8601 timestamp of when request was created
- `start_date`: ISO 8601 start of date range (optional)
- `end_date`: ISO 8601 end of date range (optional)

#### get_thaw_request() - Lines 1094-1119
Retrieves a specific thaw request by ID:

```python
def get_thaw_request(client: Elasticsearch, request_id: str) -> dict:
    """Retrieve a thaw request from the status index by ID."""
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Retrieving thaw request %s", request_id)

    try:
        response = client.get(index=STATUS_INDEX, id=request_id)
        return response["_source"]
    except NotFoundError:
        loggit.error("Thaw request %s not found", request_id)
        raise ActionError(f"Thaw request {request_id} not found")
    except Exception as e:
        loggit.error("Failed to retrieve thaw request %s: %s", request_id, e)
        raise ActionError(f"Failed to retrieve thaw request {request_id}: {e}")
```

#### list_thaw_requests() - Lines 1122-1149
Retrieves all thaw requests:

```python
def list_thaw_requests(client: Elasticsearch) -> list[dict]:
    """List all thaw requests from the status index."""
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Listing all thaw requests")

    query = {"query": {"term": {"doctype": "thaw_request"}}, "size": 10000}

    try:
        response = client.search(index=STATUS_INDEX, body=query)
        requests = response["hits"]["hits"]
        loggit.debug("Found %d thaw requests", len(requests))
        return [{"id": req["_id"], **req["_source"]} for req in requests]
    except NotFoundError:
        loggit.warning("Status index not found")
        return []
    except Exception as e:
        loggit.error("Failed to list thaw requests: %s", e)
        raise ActionError(f"Failed to list thaw requests: {e}")
```

#### update_thaw_request() - Lines 1152-1185
Updates a thaw request in the status index:

```python
def update_thaw_request(
    client: Elasticsearch, request_id: str, status: str = None, **fields
) -> None:
    """Update a thaw request in the status index."""
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Updating thaw request %s", request_id)

    update_doc = {}
    if status:
        update_doc["status"] = status
    update_doc.update(fields)

    try:
        client.update(index=STATUS_INDEX, id=request_id, doc=update_doc)
        loggit.info("Thaw request %s updated successfully", request_id)
    except Exception as e:
        loggit.error("Failed to update thaw request %s: %s", request_id, e)
        raise ActionError(f"Failed to update thaw request {request_id}: {e}")
```

---

## 2. THAW REQUEST STATUS LIFECYCLE

### Location: `curator/actions/deepfreeze/constants.py`

```python
# Thaw request status lifecycle
THAW_STATUS_IN_PROGRESS = "in_progress"  # Thaw operation is actively running
THAW_STATUS_COMPLETED = "completed"      # Thaw completed, data available and mounted
THAW_STATUS_FAILED = "failed"            # Thaw operation failed
THAW_STATUS_REFROZEN = "refrozen"        # Thaw was completed but has been refrozen (cleaned up)

THAW_REQUEST_STATUSES = [
    THAW_STATUS_IN_PROGRESS,
    THAW_STATUS_COMPLETED,
    THAW_STATUS_FAILED,
    THAW_STATUS_REFROZEN,
]
```

### Status Flow:
1. **in_progress** → Initial state when thaw request is created
2. **completed** → Set after all repositories have been mounted and indices are restored
3. **refrozen** → Set when user explicitly refreezes or cleanup runs
4. **failed** → Set if thaw operation fails

---

## 3. AWS S3/GLACIER API - RESTORE REQUEST TRACKING

### Location: `curator/s3client.py` - AwsS3Client.thaw()

Lines 279-388: The thaw method initiates restore requests to S3/Glacier:

```python
def thaw(
    self,
    bucket_name: str,
    base_path: str,
    object_keys: list[dict],
    restore_days: int = 7,
    retrieval_tier: str = "Standard",
) -> None:
    """
    Restores objects from Glacier storage class back to an instant access tier.
    """
    self.loggit.info(
        "Starting thaw operation - bucket: %s, base_path: %s, objects: %d, restore_days: %d, tier: %s",
        bucket_name,
        base_path,
        len(object_keys),
        restore_days,
        retrieval_tier
    )

    restored_count = 0
    skipped_count = 0
    error_count = 0

    for idx, obj in enumerate(object_keys, 1):
        key = obj.get("Key") if isinstance(obj, dict) else obj

        if not key.startswith(base_path):
            skipped_count += 1
            continue

        # Get storage class from object metadata
        if isinstance(obj, dict) and "StorageClass" in obj:
            storage_class = obj.get("StorageClass", "")
        else:
            try:
                response = self.client.head_object(Bucket=bucket_name, Key=key)
                storage_class = response.get("StorageClass", "")
            except Exception as e:
                error_count += 1
                # ... error handling
                continue

        try:
            if storage_class in ["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"]:
                self.loggit.debug(
                    "Restoring object %d/%d: %s from %s",
                    idx,
                    len(object_keys),
                    key,
                    storage_class
                )
                # Initiate S3 restore request
                self.client.restore_object(
                    Bucket=bucket_name,
                    Key=key,
                    RestoreRequest={
                        "Days": restore_days,
                        "GlacierJobParameters": {"Tier": retrieval_tier},
                    },
                )
                restored_count += 1
        except Exception as e:
            error_count += 1
            # ... error handling

    self.loggit.info(
        "Thaw operation completed - restored: %d, skipped: %d, errors: %d (total: %d)",
        restored_count,
        skipped_count,
        error_count,
        len(object_keys)
    )
```

**S3 API Call Details:**
- Uses `client.restore_object()` boto3 call
- Specifies `Days` parameter (restore_days, e.g., 7 days)
- Specifies `GlacierJobParameters.Tier` (Standard/Expedited/Bulk)
- Returns immediately - does NOT wait for restore to complete
- The restore request is tracked by AWS, not directly in Curator

---

## 4. CHECKING IF RESTORE IS IN PROGRESS vs COMPLETED

### Location: `curator/actions/deepfreeze/utilities.py` - check_restore_status()

Lines 852-991: Uses S3 head_object() to check the Restore metadata header:

```python
def check_restore_status(s3: S3Client, bucket: str, base_path: str) -> dict:
    """
    Check the restoration status of objects in an S3 bucket.

    Uses head_object to check the Restore metadata field, which is the only way
    to determine if a Glacier object has been restored (storage class remains GLACIER
    even after restoration).

    This function uses parallel processing to check multiple objects concurrently,
    significantly improving performance when checking large numbers of objects.
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.debug("Checking restore status for s3://%s/%s", bucket, base_path)

    # ... code to normalize path and list objects ...

    # Helper function to check a single Glacier object's restore status
    def check_single_object(key: str) -> tuple:
        """Check restore status for a single object. Returns (status, key)."""
        try:
            metadata = s3.head_object(bucket, key)
            restore_header = metadata.get("Restore")

            if restore_header:
                # Restore header exists - parse it to check status
                # Format: 'ongoing-request="true"' or 'ongoing-request="false", expiry-date="..."'
                if 'ongoing-request="true"' in restore_header:
                    loggit.debug("Object %s: restoration in progress", key)
                    return ("in_progress", key)
                else:
                    # ongoing-request="false" means restoration is complete
                    loggit.debug("Object %s: restored (expiry in header)", key)
                    return ("restored", key)
            else:
                # No Restore header means object is in Glacier and not being restored
                loggit.debug("Object %s: in Glacier, not restored", key)
                return ("not_restored", key)

        except Exception as e:
            loggit.warning("Failed to check restore status for %s: %s", key, e)
            return ("not_restored", key)

    # Check Glacier objects in parallel
    restored_count = instant_access_count
    in_progress_count = 0
    not_restored_count = 0

    max_workers = min(15, len(glacier_objects))

    loggit.debug(
        "Checking %d Glacier objects using %d workers",
        len(glacier_objects),
        max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(check_single_object, key): key for key in glacier_objects
        }

        for future in as_completed(future_to_key):
            status_result, key = future.result()

            if status_result == "restored":
                restored_count += 1
            elif status_result == "in_progress":
                in_progress_count += 1
            else:
                not_restored_count += 1

    status = {
        "total": total_count,
        "restored": restored_count,
        "in_progress": in_progress_count,
        "not_restored": not_restored_count,
        "complete": (restored_count == total_count) if total_count > 0 else False,
    }

    loggit.debug("Restore status: %s", status)
    return status
```

### Key Points about Restore Status Checking:

1. **Restore Header Format (from AWS S3 API):**
   - `ongoing-request="true"` → Restore is still in progress
   - `ongoing-request="false", expiry-date="ISO8601"` → Restore complete, expires at date

2. **No Restore Header:**
   - Object is still in Glacier storage
   - restore_object() call either wasn't made or failed

3. **Parallel Processing:**
   - Uses ThreadPoolExecutor with up to 15 concurrent workers
   - Boto3 S3 client is thread-safe
   - Significantly improves performance for large object counts

4. **Storage Class Behavior:**
   - Objects remain GLACIER storage class even after restore
   - The Restore header is the authoritative indicator

---

## 5. THAW REQUEST STATUS MONITORING IN THAW ACTION

### Location: `curator/actions/deepfreeze/thaw.py`

#### Thaw Operation Flow (do_action method):

**Lines 1164-1442: Create mode with async/sync support**

Lines 1262-1273: Save thaw request for sync mode:
```python
# Save thaw request for status tracking (will be marked completed when done)
save_thaw_request(
    self.client,
    self.request_id,
    thawed_repos,
    "in_progress",
    self.start_date,
    self.end_date,
)
```

Lines 1425-1436: Save thaw request for async mode:
```python
# Save thaw request for later querying
save_thaw_request(
    self.client,
    self.request_id,
    thawed_repos,
    "in_progress",
    self.start_date,
    self.end_date,
)
```

#### do_check_status() - Checking and Mounting (Lines 341-519)

**Step 1: Check restoration status using check_restore_status()**
```python
status = check_restore_status(self.s3, repo.bucket, repo.base_path)
# status dict contains: total, restored, in_progress, not_restored, complete
```

**Step 2: Mount repository when complete**
```python
if status["complete"]:
    self.loggit.info("Restoration complete for %s, mounting...", repo.name)
    mount_repo(self.client, repo)
    self._update_repo_dates(repo)
    mounted_count += 1
    newly_mounted_repos.append(repo)
else:
    all_complete = False
```

**Step 3: Mount indices in date range**
```python
should_mount_indices = (
    all_complete
    and start_date_str
    and end_date_str
    and any(repo.is_mounted for repo in repos)
)

if should_mount_indices:
    # Find and mount indices within the date range
    mount_result = find_and_mount_indices_in_date_range(
        self.client, mounted_repos, start_date, end_date
    )
```

**Step 4: Update thaw request status**
```python
if all_complete:
    update_thaw_request(self.client, self.check_status, status="completed")
```

---

## 6. EXPIRATION/TIMEOUT LOGIC

### Location: `curator/actions/deepfreeze/thaw.py` - _thaw_repository()

Lines 200-210: Repository expiration timestamp is set:
```python
from datetime import timedelta, timezone

expires_at = datetime.now(timezone.utc) + timedelta(days=self.duration)
repo.start_thawing(expires_at)
repo.persist(self.client)
```

### Location: `curator/actions/deepfreeze/cleanup.py` - _detect_and_mark_expired_repos()

Lines 158-287: Automatic expiration detection:

```python
def _detect_and_mark_expired_repos(self) -> int:
    """
    Detect repositories whose S3 restore has expired and mark them as expired.
    
    Checks repositories in two ways:
    1. Thawed repos with expires_at timestamp that has passed
    2. Mounted repos (regardless of state) by checking S3 restore status directly
    """
    now = datetime.now(timezone.utc)
    expired_count = 0
    checked_repos = set()

    # METHOD 1: Check thawed repos with expires_at timestamp
    for repo in thawed_repos:
        if repo.name in checked_repos:
            continue

        if repo.expires_at:
            expires_at = repo.expires_at
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            if expires_at <= now:
                # Mark as expired
                ...
```

### Repository State Machine:

```
ACTIVE (never thawed) 
  ↓
FROZEN (in cold storage, not accessible)
  ↓
THAWING (S3 restore in progress, waiting for retrieval)
  ↓
THAWED (S3 restore complete, mounted and in use)
  ↓
EXPIRED (S3 restore expired, reverted to Glacier, ready for cleanup)
```

---

## 7. ONGOING-REQUEST HEADER HANDLING

### Location: `curator/actions/deepfreeze/utilities.py` - check_restore_status()

Lines 928-943: Parsing the Ongoing-request header:

```python
if restore_header:
    # Restore header exists - parse it to check status
    # Format: 'ongoing-request="true"' or 'ongoing-request="false", expiry-date="..."'
    if 'ongoing-request="true"' in restore_header:
        loggit.debug("Object %s: restoration in progress", key)
        return ("in_progress", key)
    else:
        # ongoing-request="false" means restoration is complete
        loggit.debug("Object %s: restored (expiry in header)", key)
        return ("restored", key)
else:
    # No Restore header means object is in Glacier and not being restored
    loggit.debug("Object %s: in Glacier, not restored", key)
    return ("not_restored", key)
```

### AWS S3 Restore Header Format:
When you call restore_object() on a Glacier object, S3 returns a Restore header in subsequent head_object() calls:

**Format (as returned by AWS S3):**
```
ongoing-request="true"
```
OR
```
ongoing-request="false", expiry-date="Wed, 19 Jan 2025 19:00:00 GMT"
```

**Parsing Logic:**
- If header contains `ongoing-request="true"` → Restoration is in progress
- If header contains `ongoing-request="false"` → Restoration is complete, will expire at expiry-date
- If no Restore header → Object is still in Glacier, restore hasn't been requested

---

## 8. THAW REQUEST STATUS REFREEZING

### Location: `curator/actions/deepfreeze/refreeze.py`

Lines 100-101: Getting completed thaw requests:
```python
def _get_open_thaw_requests(self) -> list:
    """Get all completed thaw requests that are eligible for refreezing."""
    all_requests = list_thaw_requests(self.client)
    return [req for req in all_requests if req.get("status") == "completed"]
```

Lines 313-399: Refreezing a single request:
```python
def _refreeze_single_request(self, request_id: str) -> dict:
    """Refreeze a single thaw request."""
    self.loggit.info("Refreezing thaw request %s", request_id)

    # Get the thaw request
    request = get_thaw_request(self.client, request_id)

    # Get repositories from request
    repo_names = request.get("repos", [])
    repos = get_repositories_by_names(self.client, repo_names)

    # For each repository:
    # 1. Delete all mounted indices
    # 2. Unmount the repository
    # 3. Delete the per-repository thawed ILM policy
    # 4. Reset repository state to frozen
    # 5. Mark thaw request as "refrozen"
```

---

## 9. THAW REQUEST FILTERING AND DISPLAY

### Location: `curator/actions/deepfreeze/thaw.py` - do_list_requests()

Lines 815-944: Listing thaw requests with filtering:

```python
def do_list_requests(self) -> None:
    """
    List thaw requests in a formatted table.
    
    By default, excludes completed and refrozen requests. Use include_completed=True to show all.
    """
    all_requests = list_thaw_requests(self.client)

    # Filter completed and refrozen requests unless explicitly included
    if not self.include_completed:
        requests = [
            req
            for req in all_requests
            if req.get("status") not in ("completed", "refrozen")
        ]
```

### Location: `curator/actions/deepfreeze/status.py` - do_repositories()

Lines 369-380: Tracking active thaw requests:

```python
# Get active thaw requests to track which repos are being thawed
active_thaw_requests = []
repos_being_thawed = set()
try:
    all_thaw_requests = list_thaw_requests(self.client)
    active_thaw_requests = [req for req in all_thaw_requests if req.get("status") == "in_progress"]
    for req in active_thaw_requests:
        repos_being_thawed.update(req.get("repos", []))
except Exception as e:
    self.loggit.warning("Could not retrieve thaw requests: %s", e)
```

---

## SUMMARY TABLE

| Aspect | Implementation |
|--------|-----------------|
| **Metadata Storage** | Elasticsearch status index (deepfreeze-status) |
| **Request ID** | UUID for each thaw request |
| **Status Tracking** | in_progress → completed → refrozen |
| **Date Range** | ISO 8601 start_date/end_date stored in request |
| **Repositories** | List of repo names in request |
| **S3 Restore Initiated** | boto3 restore_object() call with Days and Tier params |
| **Progress Check** | S3 head_object() → Restore header parsing |
| **Ongoing Status** | ongoing-request="true" or "false" in Restore header |
| **Expiry Date** | expiry-date in Restore header + expires_at in repo metadata |
| **Timeout Logic** | Manual cleanup + automatic expiration detection |
| **Repository States** | ACTIVE → FROZEN → THAWING → THAWED → EXPIRED |
| **Parallel Checking** | ThreadPoolExecutor with 15 workers for status checks |
| **Filtering** | Active (in_progress) vs Completed/Refrozen requests |

