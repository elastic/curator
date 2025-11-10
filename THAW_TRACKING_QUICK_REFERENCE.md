# Thaw Request Tracking - Quick Reference

## File Locations

| Component | File | Key Functions |
|-----------|------|---|
| **Request Management** | `curator/actions/deepfreeze/utilities.py` | `save_thaw_request()`, `get_thaw_request()`, `list_thaw_requests()`, `update_thaw_request()` |
| **Restore Status Check** | `curator/actions/deepfreeze/utilities.py` | `check_restore_status()` |
| **Thaw Action** | `curator/actions/deepfreeze/thaw.py` | `Thaw.do_action()`, `Thaw.do_check_status()`, `Thaw._thaw_repository()` |
| **S3 Client** | `curator/s3client.py` | `AwsS3Client.thaw()`, `AwsS3Client.head_object()` |
| **Status Constants** | `curator/actions/deepfreeze/constants.py` | Status and state definitions |
| **Cleanup** | `curator/actions/deepfreeze/cleanup.py` | Expiration detection and cleanup |
| **Refreeze** | `curator/actions/deepfreeze/refreeze.py` | Manual refreeze operations |

---

## Data Model

### Elasticsearch Document (deepfreeze-status index)

```python
{
    "_id": "uuid-string",                      # Request ID
    "doctype": "thaw_request",
    "request_id": "uuid-string",
    "repos": ["repo-name-1", "repo-name-2"],  # Repository names
    "status": "in_progress",                   # in_progress|completed|failed|refrozen
    "created_at": "2025-01-15T10:00:00Z",
    "start_date": "2025-01-01T00:00:00Z",      # Optional: date range filter
    "end_date": "2025-01-31T23:59:59Z"         # Optional: date range filter
}
```

---

## Key Functions Reference

### save_thaw_request()
**Location:** `utilities.py` line 1040

Creates a new thaw request document in Elasticsearch.

```python
save_thaw_request(
    client,              # Elasticsearch client
    request_id,          # UUID
    repos,               # List[Repository]
    status,              # "in_progress", "completed", etc.
    start_date,          # datetime (optional)
    end_date             # datetime (optional)
)
```

### get_thaw_request()
**Location:** `utilities.py` line 1094

Retrieves a thaw request by ID.

```python
request = get_thaw_request(client, request_id)
# Returns: dict with request data
```

### list_thaw_requests()
**Location:** `utilities.py` line 1122

Lists all thaw requests (up to 10,000).

```python
requests = list_thaw_requests(client)
# Returns: list of dicts, each with "id" + source fields
```

### update_thaw_request()
**Location:** `utilities.py` line 1152

Updates thaw request status.

```python
update_thaw_request(client, request_id, status="completed")
```

### check_restore_status()
**Location:** `utilities.py` line 852

Checks S3 Glacier restore status for all objects in a path.

```python
status = check_restore_status(s3, bucket, base_path)
# Returns: {
#     "total": 150,
#     "restored": 75,        # Restore header: ongoing-request="false"
#     "in_progress": 50,     # Restore header: ongoing-request="true"
#     "not_restored": 25,    # No Restore header
#     "complete": False
# }
```

**Performance:** Uses ThreadPoolExecutor with 15 concurrent workers for parallel head_object checks.

---

## S3/Glacier Status Tracking

### The "Restore" Header

This is what AWS S3 returns in head_object() response:

| State | Restore Header | StorageClass |
|-------|---|---|
| Not restored | (absent) | GLACIER |
| Restoring | `ongoing-request="true"` | GLACIER |
| Restored | `ongoing-request="false", expiry-date="..."` | GLACIER |
| Expired | (absent) | GLACIER |

**Key Point:** StorageClass stays GLACIER throughout - only Restore header changes.

### boto3 S3 API Calls

#### Initiate Restore
```python
# From s3client.py line 351
s3.restore_object(
    Bucket=bucket_name,
    Key=key,
    RestoreRequest={
        "Days": restore_days,              # e.g., 7
        "GlacierJobParameters": {"Tier": retrieval_tier}  # Standard/Expedited/Bulk
    }
)
```

#### Check Status
```python
# From utilities.py line 927
metadata = s3.head_object(Bucket=bucket, Key=key)
restore_header = metadata.get("Restore")
# Parse ongoing-request="true" or "false"
```

---

## Status Lifecycle

```
Created (in_progress)
    ↓
S3 restores objects in background
    ↓
All objects restored
    ↓
User checks status / Auto-check in sync mode
    ↓
Repositories mounted, indices mounted (completed)
    ↓
Data available for 7+ days
    ↓
User calls refreeze OR cleanup detects expiration
    ↓
Repositories unmounted, state reset to frozen (refrozen)
    ↓
Done
```

---

## Important Constants

From `curator/actions/deepfreeze/constants.py`:

```python
# Thaw request statuses
THAW_STATUS_IN_PROGRESS = "in_progress"
THAW_STATUS_COMPLETED = "completed"
THAW_STATUS_FAILED = "failed"
THAW_STATUS_REFROZEN = "refrozen"

# Repository thaw states
THAW_STATE_ACTIVE = "active"      # Never thawed
THAW_STATE_FROZEN = "frozen"      # In cold storage
THAW_STATE_THAWING = "thawing"    # Restore in progress
THAW_STATE_THAWED = "thawed"      # Restore complete, mounted
THAW_STATE_EXPIRED = "expired"    # Restore expired, ready for cleanup
```

---

## Expiration & Timeout Logic

### Repository Expiration Timestamp
**Set at:** `thaw.py` line 203

```python
from datetime import timedelta, timezone

expires_at = datetime.now(timezone.utc) + timedelta(days=self.duration)
repo.start_thawing(expires_at)
repo.persist(client)
```

### Automatic Expiration Detection
**In:** `cleanup.py` line 158

```python
now = datetime.now(timezone.utc)
if repo.expires_at and repo.expires_at <= now:
    # Mark as expired
```

### AWS S3 Expiration
- S3 automatically removes temporary restored copy after expiry-date
- Objects silently revert to Glacier
- No action needed from Curator

---

## Thaw Request Filtering

### In Status Displays

**Active only (default):**
```python
requests = [req for req in all_requests if req.get("status") == "in_progress"]
```

**Non-completed (includes in_progress, failed):**
```python
requests = [req for req in all_requests if req.get("status") not in ("completed", "refrozen")]
```

**All requests (with --include-completed flag):**
```python
requests = all_requests
```

---

## Common Operations

### Create a Thaw Request
```
curator_cli deepfreeze thaw --start-date 2025-01-01 --end-date 2025-01-31 --sync
```

### Check Status of Specific Request
```
curator_cli deepfreeze thaw --check-status <UUID>
```

### Check Status of All Requests
```
curator_cli deepfreeze thaw --check-status ""
```

### List Active Requests
```
curator_cli deepfreeze thaw --list-requests
```

### List All Requests (including completed)
```
curator_cli deepfreeze thaw --list-requests --include-completed
```

### Manually Refreeze
```
curator_cli deepfreeze refreeze --thaw-request-id <UUID>
```

### Refreeze All Completed Requests
```
curator_cli deepfreeze refreeze
```

---

## Error Handling

### Common Errors in check_restore_status()

| Scenario | Handling |
|----------|----------|
| head_object() fails | Count as "not_restored", log warning, continue |
| Restore header parse fails | Default to not restored |
| No objects found | Return complete=True (all 0 objects restored) |
| ThreadPoolExecutor exception | Log error, skip object, continue |

### Common Errors in save_thaw_request()

| Scenario | Handling |
|----------|----------|
| Status index doesn't exist | Raises ActionError |
| ES index() fails | Raises ActionError, logs error |
| Invalid status value | Stored as-is, not validated |

---

## Performance Considerations

### Parallel Object Checking
- Uses ThreadPoolExecutor with max 15 workers
- Each worker calls head_object() for one object
- Significantly faster than sequential checking
- Example: 1000 objects checked in ~60 seconds vs ~1000 seconds sequentially

### Query Performance
- All queries use direct ID lookup where possible
- List queries limited to 10,000 results
- Status checks are independent (no inter-repository dependencies)

---

## Testing Thaw Tracking

### Check a Specific Request
```python
from curator.actions.deepfreeze.utilities import get_thaw_request

request = get_thaw_request(es_client, request_id)
print(f"Status: {request['status']}")
print(f"Repos: {request['repos']}")
```

### List All Requests
```python
from curator.actions.deepfreeze.utilities import list_thaw_requests

requests = list_thaw_requests(es_client)
for req in requests:
    print(f"{req['id']}: {req['status']}")
```

### Check Restore Status
```python
from curator.actions.deepfreeze.utilities import check_restore_status
from curator.s3client import s3_client_factory

s3 = s3_client_factory("aws")
status = check_restore_status(s3, bucket, base_path)
print(f"Restored: {status['restored']}/{status['total']}")
```

