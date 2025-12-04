# Thaw Request Tracking Flow Diagram

## Complete Lifecycle of a Thaw Request

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ USER INITIATES THAW                                                         │
│ curator_cli deepfreeze thaw --start-date 2025-01-01 --end-date 2025-01-31  │
└────────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: THAW INITIALIZATION (thaw.py - do_action)                         │
│                                                                             │
│ 1. Generate UUID for request_id                                           │
│ 2. Find repositories by date range                                        │
│ 3. For each repository:                                                   │
│    - List S3 objects (list_objects)                                       │
│    - Call S3 restore_object() with Days=7, Tier=Standard                 │
│                                                                             │
│ Storage in Elasticsearch (deepfreeze-status index):                       │
│ {                                                                           │
│   "_id": "uuid-1234-5678",                                               │
│   "doctype": "thaw_request",                                              │
│   "request_id": "uuid-1234-5678",                                        │
│   "repos": ["deepfreeze-000001", "deepfreeze-000002"],                  │
│   "status": "in_progress",                                               │
│   "created_at": "2025-01-15T10:00:00Z",                                 │
│   "start_date": "2025-01-01T00:00:00Z",                                 │
│   "end_date": "2025-01-31T23:59:59Z"                                    │
│ }                                                                           │
└────────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
                    ▼                         ▼
          ┌──────────────────────┐  ┌──────────────────────┐
          │ SYNC MODE            │  │ ASYNC MODE           │
          │ (--sync flag)        │  │ (default)            │
          │                      │  │                      │
          │ Wait for restoration │  │ Return immediately   │
          │ and mounting         │  │ with request_id      │
          └──────────┬───────────┘  └──────────┬───────────┘
                     │                          │
                     └──────────────┬───────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: S3 GLACIER RESTORE IN PROGRESS                                    │
│                                                                             │
│ AWS S3 is restoring objects in the background                             │
│ - Time depends on retrieval tier (Standard: hours, Expedited: minutes)    │
│ - Objects remain in GLACIER storage class (StorageClass doesn't change)   │
│ - S3 adds "Restore" header to object metadata                             │
│                                                                             │
│ Current S3 Restore header state for each object:                          │
│   ongoing-request="true"                                                   │
└────────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 3: USER CHECKS STATUS (or automatic check in sync mode)             │
│                                                                             │
│ curator_cli deepfreeze thaw --check-status uuid-1234-5678                 │
│  OR (in sync mode, happens automatically)                                  │
│                                                                             │
│ Action: thaw.py - do_check_status()                                       │
│ 1. Retrieve thaw request from ES                                          │
│ 2. Get repositories from request                                          │
│ 3. For EACH repository:                                                   │
│    ┌──────────────────────────────────────────────────────┐               │
│    │ check_restore_status(s3, bucket, base_path)         │               │
│    │ Parallel check using ThreadPoolExecutor (15 workers)│               │
│    │ For EACH object in base_path:                       │               │
│    │   - Call s3.head_object(bucket, key)                │               │
│    │   - Extract Restore header from metadata            │               │
│    │   - Parse ongoing-request value:                    │               │
│    │     * "true"  → in_progress                         │               │
│    │     * "false" → restored (complete)                 │               │
│    │                                                      │               │
│    │ Returns: {                                           │               │
│    │   "total": 150,                                      │               │
│    │   "restored": 75,    # ongoing-request="false"      │               │
│    │   "in_progress": 50, # ongoing-request="true"       │               │
│    │   "not_restored": 25,# No Restore header            │               │
│    │   "complete": false                                  │               │
│    │ }                                                    │               │
│    └──────────────────────────────────────────────────────┘               │
│                                                                             │
│ 4. If status["complete"] == true for all repos:                          │
│    a. Mount each repository in Elasticsearch                              │
│    b. Update repository date ranges                                       │
│    c. Find and mount indices within date range                            │
│    d. Add indices back to data streams if applicable                      │
│    e. update_thaw_request(status="completed")                             │
└────────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 4: RESTORATION COMPLETE                                              │
│                                                                             │
│ Thaw request status updated in ES:                                        │
│ {                                                                           │
│   "_id": "uuid-1234-5678",                                               │
│   "doctype": "thaw_request",                                              │
│   "request_id": "uuid-1234-5678",                                        │
│   "repos": ["deepfreeze-000001", "deepfreeze-000002"],                  │
│   "status": "completed",  ◄─── CHANGED                                    │
│   "created_at": "2025-01-15T10:00:00Z",                                 │
│   "start_date": "2025-01-01T00:00:00Z",                                 │
│   "end_date": "2025-01-31T23:59:59Z"                                    │
│ }                                                                           │
│                                                                             │
│ Repository state in Elasticsearch:                                        │
│ {                                                                           │
│   "name": "deepfreeze-000001",                                            │
│   "thaw_state": "thawed",                                                 │
│   "is_mounted": true,                                                     │
│   "expires_at": "2025-01-22T10:00:00Z",  ◄─── restore_days = 7 days     │
│   "bucket": "my-bucket",                                                  │
│   "base_path": "curator-snapshots"                                        │
│ }                                                                           │
│                                                                             │
│ S3 Restore header state for restored objects:                            │
│   ongoing-request="false", expiry-date="Wed, 22 Jan 2025 10:00:00 GMT"  │
└────────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 5: DATA AVAILABLE & IN USE (7 days)                                 │
│                                                                             │
│ - Indices are mounted and searchable                                      │
│ - Temporary restored copies in Standard tier are available                │
│ - Will auto-expire on 2025-01-22T10:00:00Z                               │
└────────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                      ┌──────────┴──────────┐
                      │                     │
                      ▼                     ▼
          ┌──────────────────────┐ ┌──────────────────────┐
          │ USER REFREEZE        │ │ AUTOMATIC CLEANUP    │
          │ (manual)             │ │ (scheduled)          │
          │                      │ │                      │
          │ curator_cli          │ │ cleanup.py           │
          │   deepfreeze         │ │   detects expires_at │
          │   refreeze \         │ │   <= now             │
          │   uuid-1234-5678     │ │                      │
          └──────────┬───────────┘ └──────────┬───────────┘
                     │                        │
                     └────────────┬────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 6: CLEANUP                                                            │
│                                                                             │
│ For each repository in the thaw request:                                  │
│ 1. Delete all mounted indices from ES                                     │
│ 2. Unmount the repository from ES                                         │
│ 3. Delete per-repo thawed ILM policy                                      │
│ 4. Reset repository state to frozen                                       │
│ 5. NOTE: S3 objects NOT deleted, they revert to Glacier automatically     │
│                                                                             │
│ Thaw request marked as "refrozen" in ES:                                  │
│ {                                                                           │
│   "_id": "uuid-1234-5678",                                               │
│   "doctype": "thaw_request",                                              │
│   "request_id": "uuid-1234-5678",                                        │
│   "repos": ["deepfreeze-000001", "deepfreeze-000002"],                  │
│   "status": "refrozen",  ◄─── CHANGED                                     │
│   "created_at": "2025-01-15T10:00:00Z",                                 │
│   "start_date": "2025-01-01T00:00:00Z",                                 │
│   "end_date": "2025-01-31T23:59:59Z"                                    │
│ }                                                                           │
│                                                                             │
│ Repository state reverted:                                                │
│ {                                                                           │
│   "name": "deepfreeze-000001",                                            │
│   "thaw_state": "frozen",  ◄─── CHANGED                                   │
│   "is_mounted": false,                                                    │
│   "expires_at": null,                                                     │
│   "bucket": "my-bucket",                                                  │
│   "base_path": "curator-snapshots"                                        │
│ }                                                                           │
│                                                                             │
│ S3 state (automatic, no action by Curator):                              │
│   - Temporary restored copy expires automatically                         │
│   - Objects revert to GLACIER storage class                               │
│   - Restore header removed from metadata                                  │
└────────────────────────────────┬──────────────────────────────────────────────┘
                                 │
                                 ▼
                        REQUEST LIFECYCLE COMPLETE
                    (Can be viewed with --include-completed)
```

---

## Key Technical Details

### The "Restore" Header from S3

This is the ONLY reliable way to track Glacier restore status:

```
DURING RESTORATION:
HEAD /object.json
Response headers:
  Restore: ongoing-request="true"
  StorageClass: GLACIER

AFTER RESTORATION COMPLETE:
HEAD /object.json
Response headers:
  Restore: ongoing-request="false", expiry-date="Wed, 22 Jan 2025 10:00:00 GMT"
  StorageClass: GLACIER  ◄─── Still GLACIER!

AFTER EXPIRATION:
HEAD /object.json
Response headers:
  (Restore header removed)
  StorageClass: GLACIER
```

### Why Storage Class Doesn't Change

- Objects in GLACIER remain in GLACIER storage class even after restoration
- This is by design - AWS tracks restoration separately via the Restore header
- Once the restore expires, objects silently revert to cold storage
- No explicit "refreeze" action needed on S3 side

### Parallel Status Checking

The check_restore_status() function uses ThreadPoolExecutor to check multiple objects concurrently:

```python
with ThreadPoolExecutor(max_workers=min(15, len(glacier_objects))) as executor:
    # Submit all head_object checks concurrently
    # Collate results as they complete
    # Much faster than sequential checking (e.g., 1000 objects in seconds vs minutes)
```

---

## Status Transitions

```
                     ┌─────────────────────────────────────────┐
                     │ INITIAL: in_progress                    │
                     │ (all thaw requests start here)          │
                     └──────────────────────────────────────────┘
                                    │
                    ┌───────────────┴────────────────┐
                    │                                │
                    ▼                                ▼
         ┌─────────────────────┐        ┌─────────────────────┐
         │ Restoration complete│        │ S3 operations fail  │
         │ Indices mounted     │        │ or timeout          │
         └──────────┬──────────┘        └──────────┬──────────┘
                    │                              │
                    ▼                              ▼
         ┌─────────────────────┐        ┌─────────────────────┐
         │ SUCCESSFUL:         │        │ FAILED:             │
         │ completed           │        │ failed              │
         └──────────┬──────────┘        └─────────────────────┘
                    │
         (User calls refreeze OR
          cleanup detects expiration)
                    │
                    ▼
         ┌─────────────────────┐
         │ DONE:               │
         │ refrozen            │
         │ (old requests)      │
         └─────────────────────┘
```

