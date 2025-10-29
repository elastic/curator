# Cleanup Action

## Purpose

The Cleanup action is an automatic maintenance process that detects and processes expired thawed repositories, cleaning up resources that are no longer needed. It runs as part of the rotation workflow and can also be run independently on a schedule.

Cleanup handles:
1. **Expired Repository Detection**: Identifies repositories whose AWS Glacier restore has expired
2. **Repository Unmounting**: Removes expired repositories from Elasticsearch
3. **Index Deletion**: Removes searchable snapshots that only exist in expired repositories
4. **Thaw Request Management**: Cleans up old completed, failed, and refrozen requests
5. **ILM Policy Cleanup**: Removes orphaned thawed ILM policies

**Key Concept**: When you thaw data from Glacier, AWS provides temporary access for a duration (e.g., 7 days). After this expires, Cleanup detects the expiration and unmounts repositories automatically, freeing Elasticsearch resources.

## Prerequisites

### System Requirements

1. **Deepfreeze Initialized**
   - Setup action must have been run successfully
   - `deepfreeze-status` index must exist

2. **Elasticsearch Permissions**
   - `snapshot.delete_repository` - Unmount repositories
   - `indices.delete` - Delete indices
   - `ilm.delete_policy` - Delete ILM policies
   - Read/write access to `deepfreeze-status` index

3. **AWS Credentials** (for S3 status checks)
   - `s3:GetObjectAttributes` - Check restore status
   - `s3:ListBucket` - List objects in repositories

### When to Run

**Automatically**:
- After every rotation (built-in)
- Part of scheduled rotation workflow

**Manually**:
- On-demand maintenance
- After manual thaw operations
- Testing/verification

**Scheduled** (Recommended):
- Daily cron job: `0 3 * * * curator_cli deepfreeze cleanup`
- Catches expirations between rotations

## Effects

### Detection Phase

#### 1. Timestamp-Based Detection

For repositories in `thawed` state:
- Compares current time to `expires_at` timestamp
- Marks as `expired` if current time ≥ `expires_at`
- Updates repository state in `deepfreeze-status` index

#### 2. S3-Based Detection

For mounted repositories (any state):
- Queries S3 for actual object restore status
- Checks: restored, in progress, not restored
- Marks as `expired` if all objects are NOT restored
- Handles edge cases where timestamp is missing or stale

**Why Both Methods?**
- Timestamp: Fast, efficient for normal cases
- S3 Check: Catches anomalies (clock skew, manual S3 operations, missing timestamps)

### Cleanup Phase

For each repository in `expired` state:

#### 1. Verify Mount Status

- Queries Elasticsearch to confirm actual mount status
- Handles state desync (in-memory flag vs actual cluster state)
- Safety check to prevent errors

#### 2. Unmount Repository

If mounted:
- Unregisters from Elasticsearch: `DELETE /_snapshot/{repo_name}`
- Logs success
- Continues cleanup even if unmount fails (handles already-unmounted cases)

#### 3. Reset Repository State

- State transition: `expired` → `frozen`
- Clears `is_mounted` flag
- Clears `expires_at` timestamp
- Persists to `deepfreeze-status` index

#### 4. Identify Indices to Delete

**Safety Logic**:
- Scans snapshots in expired repositories
- Checks if index exists in Elasticsearch
- Checks if index has snapshots in OTHER repositories
- **Only deletes if**: Index exists ONLY in expired repositories

**Index Naming Patterns**:
- Original names (e.g., `.ds-df-test-2024.01.01-000001`)
- Partial prefix (e.g., `partial-.ds-df-test-2024.01.01-000001`)
- Restored prefix (e.g., `restored-.ds-df-test-2024.01.01-000001`)

#### 5. Delete Indices

For each identified index:
- Validates index still exists (double-check)
- Gets index health for audit trail
- Deletes index: `DELETE /{index_name}`
- Logs success or failure
- Continues with remaining indices even if one fails

### Maintenance Phase

#### 6. Clean Up Old Thaw Requests

**Retention Policies** (configurable in settings):
- Completed requests: Default 7 days
- Failed requests: Default 7 days
- Refrozen requests: Default 35 days

**Cleanup Logic**:
- Calculates age from `created_at` timestamp
- Deletes requests older than retention period
- Also deletes stale `in_progress` requests where all repos are no longer thawed

#### 7. Clean Up Orphaned Thawed ILM Policies

**Detection**:
- Finds all policies ending with `-thawed`
- Filters to policies matching deepfreeze prefix
- Checks if policy has any indices or datastreams assigned

**Deletion**:
- Deletes policies with zero usage
- Keeps policies still in use (will clean up later)

## Options

Cleanup has no user-configurable options. Behavior is controlled by:

1. **Repository State**: Only processes `expired` repositories
2. **Retention Settings**: Stored in `deepfreeze-status` index (from setup)
3. **Safety Checks**: Built-in (cannot delete indices with snapshots elsewhere)

### Internal Configuration

These settings are stored in `deepfreeze-status` index (configured during setup):

- `thaw_request_retention_days_completed`: Default 7
- `thaw_request_retention_days_failed`: Default 7
- `thaw_request_retention_days_refrozen`: Default 35

## Usage Examples

### Manual Cleanup

```bash
# Run cleanup manually
curator_cli deepfreeze cleanup

# Logs show:
# - Expired repositories detected
# - Repositories unmounted
# - Indices deleted
# - Thaw requests cleaned up
# - ILM policies removed
```

### Dry Run

```bash
# Preview what cleanup would do
curator_cli deepfreeze cleanup --dry-run

# Output shows:
# - Repositories that would be marked expired
# - Repositories that would be unmounted
# - Indices that would be deleted
# - Thaw requests that would be removed
# - No actual changes made
```

### Scheduled Cleanup (Cron)

```bash
# /etc/cron.d/deepfreeze-cleanup
# Run cleanup daily at 3 AM
0 3 * * * curator_cli deepfreeze cleanup >> /var/log/deepfreeze-cleanup.log 2>&1
```

### Cleanup After Manual Thaw

```bash
# After finishing with thawed data
curator_cli deepfreeze refreeze --thaw-request-id <id>

# Then run cleanup to process any other expirations
curator_cli deepfreeze cleanup
```

### Verify Cleanup Results

```bash
# Check for expired repos (should be none after cleanup)
curator_cli deepfreeze status --show-repos --porcelain | grep "expired"

# Check for old thaw requests (should respect retention)
curator_cli deepfreeze thaw --list-requests --include-completed
```

## Detection Logic

### Expired Repository Detection

#### Method 1: Timestamp Comparison

```
For repository in state 'thawed':
  if expires_at exists:
    if current_time >= expires_at:
      → Mark as 'expired'
```

**Advantages**:
- Fast (no S3 API calls)
- Reliable when timestamps are accurate

**Limitations**:
- Requires accurate timestamps
- Doesn't catch manual S3 operations

#### Method 2: S3 Restore Status Check

```
For repository that is mounted:
  Check S3 restore status for all objects
  Count: restored, in_progress, not_restored

  if not_restored > 0 AND restored == 0 AND in_progress == 0:
    → Mark as 'expired'
```

**Advantages**:
- Ground truth from AWS
- Catches edge cases

**Limitations**:
- Slower (S3 API calls)
- Requires S3 permissions

**Why Both?**
- Timestamp check is fast primary method
- S3 check catches anomalies
- Together they provide robust detection

### Thaw Request Cleanup Logic

```
For each thaw request:
  age = current_time - created_at

  if status == 'completed' AND age > retention_completed:
    → Delete request

  if status == 'failed' AND age > retention_failed:
    → Delete request

  if status == 'refrozen' AND age > retention_refrozen:
    → Delete request

  if status == 'in_progress':
    Check all repos in request
    if ALL repos are NOT in (thawing, thawed) state:
      → Delete request (stale)
```

### Index Deletion Safety

```
For each index in expired repositories:
  if index does NOT exist in Elasticsearch:
    → Skip (already gone)

  other_repos = all repos EXCEPT expired repos

  has_snapshots_elsewhere = False
  for repo in other_repos:
    if index exists in repo snapshots:
      has_snapshots_elsewhere = True
      break

  if has_snapshots_elsewhere:
    → Skip (has backups elsewhere)
  else:
    → DELETE (only exists in expired repos)
```

**Safety Guarantee**: Indices are only deleted if they have no snapshots in any other repository.

## Error Handling

### Common Issues

#### 1. No Expired Repositories

**Message**: `No expired repositories found to clean up`

**Cause**: All thawed repositories still within their duration

**Action**: This is normal - cleanup has nothing to do

#### 2. Repository Unmount Failed (Already Unmounted)

**Warning**: `Repository deepfreeze-000010 marked as mounted but not found in Elasticsearch`

**Cause**: State desync - in-memory flag says mounted, but ES doesn't have it

**Effect**: Non-critical - cleanup continues and corrects the state

**Action**: No user action needed (automatic correction)

#### 3. Index Deletion Failed (Already Deleted)

**Error**: `Failed to delete index partial-my-index: index_not_found_exception`

**Cause**: Index was already deleted (race condition or manual deletion)

**Effect**: Non-critical - cleanup continues

**Action**: No user action needed

#### 4. ILM Policy Deletion Failed (Still in Use)

**Warning**: `Failed to check/delete ILM policy my-policy-thawed: ...`

**Cause**: Policy has indices assigned (shouldn't happen after index deletion)

**Effect**: Non-critical - policy will be cleaned up in next run

**Action**:
```bash
# Verify policy usage
curl -X GET 'http://localhost:9200/_ilm/policy/my-policy-thawed'

# If truly orphaned, delete manually
curl -X DELETE 'http://localhost:9200/_ilm/policy/my-policy-thawed'
```

#### 5. S3 Restore Status Check Failed

**Error**: `Failed to check S3 restore status for repository: Access Denied`

**Cause**: AWS credentials lack `s3:GetObjectAttributes` permission

**Effect**: Repository not marked as expired (timestamp-based detection still works)

**Solutions**:
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObjectAttributes",
    "s3:GetObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::your-bucket-prefix*/*"
  ]
}
```

## Best Practices

### Scheduling

1. **Daily Cleanup** (Recommended)
   ```bash
   # Cron: 3 AM daily
   0 3 * * * curator_cli deepfreeze cleanup
   ```
   - Catches expirations promptly
   - Low overhead (fast operation)
   - Keeps cluster clean

2. **After Rotation** (Automatic)
   - Rotation automatically calls cleanup
   - No additional configuration needed

3. **Ad-Hoc Cleanup**
   - Run after manual operations
   - Testing/verification
   - Immediate cleanup needed

### Monitoring

1. **Log Review**
   ```bash
   # Check cleanup logs
   grep "cleanup" /var/log/curator.log | tail -20
   ```
   - Look for expired repo detection
   - Verify indices deleted
   - Check for errors

2. **Status Verification**
   ```bash
   # After cleanup, check for expired repos (should be none)
   curator_cli deepfreeze status --show-repos | grep "expired"
   ```

3. **Metrics Tracking**
   - Count expired repos per day
   - Index deletion counts
   - Thaw request cleanup counts

### Retention Tuning

Default retention periods are conservative. Adjust based on your needs:

**Short Retention** (7 days for completed/failed, 35 for refrozen):
- Keeps index smaller
- Faster queries
- Less historical audit trail

**Long Retention** (30+ days):
- Better audit trail
- Easier troubleshooting
- Larger index

**Consider**:
- Compliance requirements (audit logs)
- Troubleshooting needs (request history)
- Index size vs query performance

## Cleanup Lifecycle

### Complete Workflow

```
1. Cleanup action triggered (manual, cron, or post-rotation)
   ↓
2. DETECTION PHASE
   ↓
   a. Get all repositories matching prefix
   ↓
   b. Filter to thawed repositories
   ↓
   c. For each thawed repo:
      - Compare current time to expires_at
      - If expired, mark as 'expired' state
   ↓
   d. Get all mounted repositories
   ↓
   e. For each mounted repo:
      - Query S3 restore status
      - If all objects not restored, mark as 'expired'
   ↓
3. CLEANUP PHASE
   ↓
   a. Get all repositories in 'expired' state
   ↓
   b. For each expired repository:
      ↓
      i. Verify actual mount status from Elasticsearch
      ↓
      ii. If mounted: Unmount repository
      ↓
      iii. Reset state: expired → frozen
      ↓
      iv. Clear mount flags and timestamps
      ↓
      v. Persist state to deepfreeze-status
   ↓
   c. Identify indices to delete
      ↓
      i. Get all indices from expired repo snapshots
      ↓
      ii. Check each index exists in Elasticsearch
      ↓
      iii. Check if index has snapshots in other repos
      ↓
      iv. Flag for deletion if ONLY in expired repos
   ↓
   d. Delete flagged indices
      ↓
      i. For each index:
         - Validate still exists
         - Get health for audit
         - Delete index
         - Log result
   ↓
4. MAINTENANCE PHASE
   ↓
   a. Get all thaw requests
   ↓
   b. For each request:
      - Calculate age
      - Check status and retention policy
      - Delete if beyond retention
   ↓
   c. Get all thawed ILM policies
   ↓
   d. For each policy:
      - Check if any indices use it
      - Delete if orphaned (zero usage)
   ↓
5. Report results
```

## State Transitions

### Repository States

```
thawed (expires_at in future)
   ↓
   [time passes, expires_at reached]
   ↓
thawed (expires_at in past)
   ↓
   [cleanup detection phase]
   ↓
expired
   ↓
   [cleanup unmount phase]
   ↓
frozen
```

### Thaw Request States

```
in_progress (recent)
   ↓
   [user completes work]
   ↓
completed
   ↓
   [retention period: 7 days]
   ↓
deleted by cleanup

OR

in_progress
   ↓
   [user refreezes]
   ↓
refrozen
   ↓
   [retention period: 35 days]
   ↓
deleted by cleanup

OR

in_progress
   ↓
   [all repos cleaned up]
   ↓
stale in_progress
   ↓
   [cleanup detects stale state]
   ↓
deleted by cleanup
```

### Index Lifecycle

```
Index mounted from thawed repository
   ↓
   [expires_at reached]
   ↓
Repository marked expired
   ↓
   [cleanup runs]
   ↓
Check: Index in other repositories?
   ↓
   Yes → Index kept (safe)
   ↓
   No → Index deleted (no other backups)
```

## Comparison: Cleanup vs Refreeze

| Aspect | Cleanup | Refreeze |
|--------|---------|----------|
| **Trigger** | Automatic (scheduled) | Manual (user-initiated) |
| **Purpose** | Expired repository maintenance | Early unmount ("I'm done") |
| **Timing** | After `expires_at` reached | Any time while thawed |
| **Detection** | Timestamp + S3 status | User knows they're done |
| **Safety** | Checks index snapshots in other repos | Assumes user verified |
| **Scope** | All expired repositories | Specific thaw request(s) |
| **Request Status** | N/A (processes expired state) | `in_progress` → `refrozen` |

### When Each Runs

**Cleanup**:
- Daily cron job
- After rotation
- Manual trigger for maintenance

**Refreeze**:
- User completes analysis early
- Testing/development workflows
- Want to free resources before expiration

## Related Actions

- **Thaw**: Creates thaw requests (cleanup processes their expiration)
- **Refreeze**: Manual unmount (cleanup handles automatic unmount)
- **Rotate**: Calls cleanup automatically
- **Status**: View repository states (cleanup transitions states)

## Performance Considerations

### Operation Speed

Cleanup is typically fast:
- Detection phase: 1-5 seconds (timestamp checks)
- S3 checks: 1-2 seconds per mounted repo
- Repository unmount: < 1 second per repo
- Index deletion: 1-5 seconds per index
- Thaw request cleanup: < 1 second
- ILM policy cleanup: < 1 second per policy

**Typical Total Time**: 10-60 seconds

### Resource Impact

- **CPU**: Low (simple comparisons and API calls)
- **Memory**: Low (processes one repository at a time)
- **Network**: Moderate (S3 API calls for status checks)
- **Cluster Load**: Low-medium (index deletions may spike briefly)

### Optimization

1. **Timestamp-First Strategy**: Fast path for most cases
2. **S3 Checks Optional**: Only for mounted repos (catches edge cases)
3. **Sequential Processing**: Avoids overwhelming cluster
4. **Graceful Error Handling**: Continues on failures

### Scheduling Considerations

- **Time**: Off-peak hours (e.g., 3 AM)
- **Frequency**: Daily is sufficient for most cases
- **Conflict Avoidance**: Don't run during heavy ingestion or rotation

## Security Considerations

- **IAM Permissions**: Requires S3 read permissions (status checks)
- **Elasticsearch Permissions**: Requires delete permissions (repos, indices, policies)
- **Audit Trail**: All deletions logged
- **Safety Checks**: Won't delete indices with snapshots elsewhere
- **State Validation**: Verifies mount status before unmounting

## Retention Configuration

### Default Retention Periods

```
Completed requests: 7 days
Failed requests: 7 days
Refrozen requests: 35 days
In-progress (stale): Immediate (if all repos cleaned up)
```

### Why Different Retention?

- **Completed/Failed** (7 days): Short-term audit trail, not needed long-term
- **Refrozen** (35 days): Longer retention for cost tracking and analysis patterns
- **Stale In-Progress**: Immediate cleanup (indicates orphaned request)

### Tuning Retention

To modify retention, you would need to:
1. Update settings in `deepfreeze-status` index
2. Or modify default values in setup code
3. Future enhancement: CLI flags for retention configuration

## Index Deletion Safety

### Multi-Repository Safety

Cleanup uses a conservative approach to index deletion:

```
Example Scenario:

Repositories:
- deepfreeze-000005 (expired)
- deepfreeze-000006 (active)
- deepfreeze-000007 (active)

Index: logs-2024-12-15-000001

Snapshots:
- deepfreeze-000005: snapshot-001 contains logs-2024-12-15-000001
- deepfreeze-000006: snapshot-045 contains logs-2024-12-15-000001

Cleanup Decision:
→ DO NOT DELETE logs-2024-12-15-000001
  (has snapshot in deepfreeze-000006)

Index: logs-2024-11-20-000012

Snapshots:
- deepfreeze-000005: snapshot-023 contains logs-2024-11-20-000012
- (no other repositories have this index)

Cleanup Decision:
→ DELETE logs-2024-11-20-000012
  (only exists in expired repository)
```

**Safety Guarantee**: If an index has ANY snapshot in ANY non-expired repository, it is never deleted by cleanup.

### What This Means

- **Over-rotation**: Indices may be snapshotted multiple times across repositories
- **Safety**: Cleanup will never delete data that has backups elsewhere
- **Storage**: May keep indices longer than strictly necessary (erring on safety)
- **Manual Override**: Can manually delete indices if you know they're truly orphaned

## Dry Run Example

```bash
curator_cli deepfreeze cleanup --dry-run

# Output:

DRY-RUN: Checking for thawed repositories that have passed expiration time

DRY-RUN: Would mark 2 repositories as expired:
  - deepfreeze-000008 (expired 2 days ago at 2025-01-13T09:00:00Z)
  - deepfreeze-000009 (expired 6 hours ago at 2025-01-14T21:00:00Z)

DRY-RUN: Found 2 expired repositories to clean up

Would process 2 repositories:
  - deepfreeze-000008 (state: thawed, mounted: True)
    Would unmount and reset to frozen
    Would delete 23 mounted indices
  - deepfreeze-000009 (state: thawed, mounted: True)
    Would unmount and reset to frozen
    Would delete 31 mounted indices

DRY-RUN: Would delete 54 indices whose snapshots are only in cleaned up repositories:
  - partial-.ds-logs-2024-11-15-000001
  - partial-.ds-logs-2024-11-16-000002
  [... 52 more indices ...]

DRY-RUN: Would delete 3 old thaw requests:
  - a1b2c3d4-e5f6-7890-abcd-ef1234567890 (completed request older than 7 days)
  - b2c3d4e5-f6a7-8901-bcde-f12345678901 (refrozen request older than 35 days)
  - c3d4e5f6-a7b8-9012-cdef-123456789012 (in-progress request with no active repos)
```
