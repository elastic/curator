# Repair Metadata Action

## Purpose

The Repair Metadata action is a diagnostic and maintenance tool that detects and fixes discrepancies between metadata stored in Elasticsearch and the actual S3 storage state. It ensures that both repository metadata and thaw request metadata accurately reflect the current state in S3.

Repair Metadata handles:
1. **Repository Metadata Verification**: Scans all repositories and compares metadata with actual S3 storage class
2. **Thaw Request Metadata Verification**: Scans all in_progress thaw requests and compares with actual S3 restore status
3. **Discrepancy Detection**: Identifies metadata that doesn't match S3 reality
4. **Automatic Correction**: Updates metadata to match actual S3 state
5. **Comprehensive Reporting**: Provides detailed reports of all discrepancies and fixes

**Key Concept**: Repository metadata can become desynchronized from S3 storage state due to bugs, failed operations, or manual S3 modifications. This action provides a way to detect and correct these inconsistencies automatically.

## Background: Why This Action Exists

### The Metadata Desync Bug

Prior to version 8.0.21 (commit d015e32), the `rotate` action had a bug where it would successfully push repositories to GLACIER storage but fail to update the `thaw_state` metadata field from `'active'` to `'frozen'`. This caused a state desynchronization where:

- **S3 Reality**: Repository objects stored in GLACIER
- **Elasticsearch Metadata**: `thaw_state='active'` (incorrect)

**Impact of Desync**:
- Status displays show incorrect repository states
- Tests skip due to "no frozen repositories" when many actually exist
- Thaw operations may behave unexpectedly
- Cost tracking and auditing becomes inaccurate

### The Fix

The bug was fixed in `rotate.py` by calling `repository.reset_to_frozen()` after `push_to_glacier()`, which properly sets:
- `thaw_state = 'frozen'`
- `is_mounted = False`
- `is_thawed = False`
- Clears `thawed_at` and `expires_at` timestamps

However, repositories that were rotated before the fix still have incorrect metadata. The Repair Metadata action provides a way to correct these historical discrepancies.

### The Stale Thaw Request Problem

When thaw requests are created but never checked, their status remains `in_progress` indefinitely, even after the S3 restore has completed or expired. This causes:

**The Problem**:
- Thaw request created on 2025-11-03 with `status='in_progress'`
- S3 restore completed after a few hours → objects available
- After 7 days (default restore duration) → S3 automatically expires the restore
- Metadata still shows `status='in_progress'` (incorrect/stale)
- No way to distinguish between:
  - Actively working thaw (AWS still restoring)
  - Completed but unchecked thaw (ready to mount)
  - Expired and ignored thaw (no longer available)

**Impact of Stale Thaw Requests**:
- Clutter in thaw request list
- Cannot determine actual system state
- Resources may be wasted (repositories thawed but not mounted)
- Confusion about which thaws are still active
- Old in_progress requests accumulate over time

**The Solution**:
Repair Metadata now checks S3 Restore headers to determine the actual state:
- `ongoing-request="true"` → Truly in progress, keep as `in_progress`
- `ongoing-request="false"` → Restore complete, mark as `completed`
- **No Restore header** → Restore expired, mark as `refrozen`

This allows distinguishing between actively working thaws and stale metadata.

## Prerequisites

### System Requirements

1. **Deepfreeze Initialized**
   - Setup action must have been run successfully
   - `deepfreeze-status` index must exist
   - At least one repository must exist

2. **Elasticsearch Permissions**
   - Read access to `deepfreeze-status` index
   - Write access to `deepfreeze-status` index (to update metadata)

3. **AWS Credentials** (for S3 storage checks)
   - `s3:ListBucket` - List objects in repositories
   - `s3:GetObjectAttributes` - Check storage class of objects (optional but recommended)

### When to Run

**After Upgrading**:
- After upgrading from versions prior to 8.0.21
- To fix repositories rotated before the bug fix

**Periodic Verification**:
- Monthly verification as part of maintenance
- After manual S3 operations (storage class changes)
- After recovering from cluster failures

**Troubleshooting**:
- When status displays show unexpected states
- When tests report "no frozen repositories" but you know data exists
- When investigating cost anomalies or storage issues

**Preventive**:
- Before major operations (large thaws, migrations)
- As part of pre-upgrade validation

## Effects

### Scan Phase

#### 1. Query All Repositories

- Queries `deepfreeze-status` index for all repository documents
- Sorts by `start` date (ascending) for consistent ordering
- Retrieves: name, bucket, base_path, thaw_state, is_mounted

**Query**:
```json
{
  "query": {"term": {"doctype": "repository"}},
  "size": 1000,
  "sort": [{"start": "asc"}]
}
```

#### 2. Check S3 Storage Class

For each repository:
- Lists objects in S3 bucket at repository base_path
- Examines `StorageClass` attribute for each object
- Counts objects by storage class:
  - GLACIER, DEEP_ARCHIVE, GLACIER_IR → "glacier"
  - STANDARD, REDUCED_REDUNDANCY, etc. → "standard"
  - Default (no StorageClass) → "standard"

**Sampling Optimization**:
- Uses `MaxKeys=100` for faster checks
- Sufficient to determine repository storage state
- Full scan not needed (repositories are homogeneous)

#### 3. Determine Repository State

Based on object counts:

| Glacier Objects | Standard Objects | Total | Determination |
|----------------|------------------|-------|---------------|
| 0 | 0 | 0 | EMPTY (no objects) |
| N | 0 | N | GLACIER (all in glacier) |
| 0 | N | N | STANDARD (all in standard) |
| N | M | N+M | MIXED (some of each) |

#### 4. Compare with Metadata

For each repository:
```
expected_frozen = (metadata.thaw_state == 'frozen')
actually_frozen = (s3_state == 'GLACIER')

if expected_frozen != actually_frozen:
  → DISCREPANCY FOUND
```

### Thaw Request Scan Phase

After repository checking, the action scans all thaw requests:

#### 1. Query In-Progress Thaw Requests

- Queries `deepfreeze-status` index for thaw request documents
- Filters to only `status='in_progress'` requests
- Completed, failed, and refrozen requests are skipped (terminal states)

**Query**:
```python
all_thaw_requests = list_thaw_requests(client)
in_progress_requests = [req for req in all_thaw_requests if req.get('status') == 'in_progress']
```

#### 2. Check S3 Restore Status for Each Request

For each in_progress thaw request:
- Get all repositories listed in the request
- For each repository, call `check_restore_status()` to check S3 Restore headers
- Aggregate results to determine overall request state

**Restore Status Checking**:
```python
for repo in request_repos:
    status = check_restore_status(s3, repo.bucket, repo.base_path)
    # status contains: total, restored, in_progress, not_restored, complete
```

#### 3. Determine Actual State

Based on S3 Restore headers across all repos in the request:

| S3 Restore Status | Actual State | Meaning |
|------------------|--------------|---------|
| All objects have no Restore header | **EXPIRED** | Restore window passed, objects back in Glacier |
| All objects have `ongoing-request="false"` | **COMPLETED** | Restore done, ready to mount |
| Any object has `ongoing-request="true"` | **IN_PROGRESS** | AWS still working on restore |
| Mixed states | **MIXED** | Some repos done, some not (keep as in_progress) |
| Unable to check | **ERROR** | S3 access issues or missing repos |

#### 4. Identify Stale Metadata

Compare metadata state with actual state:

```python
if actual_state == 'EXPIRED' and metadata_state == 'in_progress':
    → STALE: should be 'refrozen'

if actual_state == 'COMPLETED' and metadata_state == 'in_progress':
    → STALE: should be 'completed'

if actual_state == 'IN_PROGRESS' and metadata_state == 'in_progress':
    → CORRECT: keep as 'in_progress'
```

### Repository Repair Phase

For each repository discrepancy (when NOT in dry-run mode):

#### 1. Fetch Repository Object

```python
repos = get_repositories_by_names(client, [repo_name])
repo = repos[0]
```

**Why Fresh Fetch?**:
- Ensures we have latest Elasticsearch state
- Avoids race conditions with concurrent operations
- Gets the full Repository object with all methods

#### 2. Update State Based on S3

**If S3 is GLACIER**:
```python
repo.reset_to_frozen()
# Sets:
#   thaw_state = 'frozen'
#   is_thawed = False
#   is_mounted = False
#   thawed_at = None
#   expires_at = None
```

**If S3 is STANDARD**:
```python
# Only update if currently marked as frozen
if metadata_state == 'frozen':
    repo.thaw_state = 'active'
    repo.is_thawed = False
```

**Why Different Logic?**:
- GLACIER → frozen: Clear, unambiguous state
- STANDARD → active: Could be active or thawed, we choose active (safe default)

#### 3. Persist to Elasticsearch

```python
repo.persist(client)
```

- Updates document in `deepfreeze-status` index
- Uses repository name as document ID
- Atomic update operation

### Thaw Request Repair Phase

For each stale thaw request (when NOT in dry-run mode):

#### 1. Update Request Status

**If actual state is EXPIRED**:
```python
update_thaw_request(client, request_id, status='refrozen')
```

- Marks request as `refrozen` since restore window has passed
- S3 objects have reverted to Glacier storage
- Cleanup action can later delete this old request

**If actual state is COMPLETED**:
```python
update_thaw_request(client, request_id, status='completed')
```

- Marks request as `completed` since restore is done
- **Important**: This does NOT mount repositories
- User must run `curator_cli deepfreeze thaw --check-status <request-id>` to mount
- A warning is logged indicating mounting is still needed

**Why Not Mount Automatically?**:
- Mounting is a complex operation involving:
  - Mounting repositories in Elasticsearch
  - Finding and mounting indices within date range
  - Adding indices back to data streams
  - Creating per-repo ILM policies
- Repair Metadata focuses on metadata correctness
- Use the dedicated thaw --check-status command for full mounting workflow

#### 2. Track Results

```python
# Count successes and failures
thaw_fixed_count += 1  # Successfully updated
thaw_failed_count += 1  # Failed to update (exception)
```

### Reporting Phase

#### Dry-Run Mode

**Rich Output** (default):
- Repository summary statistics (total, correct, discrepancies, errors)
- Thaw request summary statistics (total in_progress, correct, stale, errors)
- Table showing all repository discrepancies with:
  - Repository name
  - Current metadata state
  - Actual S3 storage class
  - Mount status
- Table showing all stale thaw requests with:
  - Request ID (shortened)
  - Repositories (first 3, with count if more)
  - Current metadata state
  - Actual S3 state
  - What it should be
  - Created date
- Warning message: "DRY-RUN: No changes made"

**Porcelain Output** (`--porcelain`):
```
TOTAL_REPOS=58
CORRECT=10
DISCREPANCIES=48
ERRORS=0
TOTAL_THAW_REQUESTS=7
CORRECT_THAW_REQUESTS=0
STALE_THAW_REQUESTS=7
THAW_REQUEST_ERRORS=0
THAW_REQUESTS_TO_FIX:
  a1b2c3d4...: metadata=in_progress, actual=EXPIRED, should_be=refrozen
  e5f6g7h8...: metadata=in_progress, actual=EXPIRED, should_be=refrozen
```

#### Live Mode

**Rich Output**:
- Same as dry-run, plus:
- Repository repair results section showing:
  - Number fixed
  - Number failed (if any)
- Thaw request repair results section showing:
  - Number fixed
  - Number failed (if any)

**Porcelain Output**:
```
[Repository stats as above...]
[Thaw request stats as above...]
FIXED=48
FAILED=0
THAW_FIXED=7
THAW_FAILED=0
```


## Options

### `--porcelain`
- **Type**: Boolean flag
- **Default**: `False`
- **Description**: Machine-readable tab-separated output
- **Use Case**: Scripting, automation, monitoring systems
- **Effect**: Disables rich formatting, outputs key=value pairs

### Dry-Run Mode

While not a direct option, repair-metadata respects the global `--dry-run` flag:

```bash
curator_cli --dry-run deepfreeze repair-metadata
```

In dry-run mode:
- No repository metadata is modified
- No documents are updated in Elasticsearch
- All discrepancies are detected and reported
- Safe to run at any time

## Usage Examples

### Basic Verification (Dry-Run)

```bash
# See what would be fixed without making changes
curator_cli --dry-run deepfreeze repair-metadata

# Output:
# Metadata Repair Report (DRY-RUN)
#
# Total repositories scanned: 58
# Repositories with correct metadata: 10
# Repositories with discrepancies: 48
#
# Discrepancies Found:
# ┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
# ┃ Repository        ┃ Metadata State ┃ Actual S3 Storage ┃ Mounted ┃
# ┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
# │ deepfreeze-000004 │ active         │ GLACIER           │ No      │
# │ deepfreeze-000005 │ active         │ GLACIER           │ No      │
# ...
#
# DRY-RUN: No changes made. Run without --dry-run to apply fixes.
```

### Fix All Discrepancies

```bash
# Actually repair the metadata
curator_cli deepfreeze repair-metadata

# Output:
# Metadata Repair Report (LIVE)
#
# Total repositories scanned: 58
# Repositories with correct metadata: 10
# Repositories with discrepancies: 48
#
# [Table showing discrepancies...]
#
# Results:
#   Fixed: 48
```

### Detect and Fix Stale Thaw Requests

```bash
# Scenario: You have 7 thaw requests created on 2025-11-03
# They were never checked and are now expired

# Step 1: Check for stale thaw requests
curator_cli --dry-run deepfreeze repair-metadata

# Output:
# Metadata Repair Report (DRY-RUN)
#
# REPOSITORIES:
#   Total scanned: 58
#   Correct metadata: 58
#   Discrepancies: 0
#
# THAW REQUESTS:
#   Total in_progress: 7
#   Correct metadata: 0
#   Stale metadata: 7
#
# Stale Thaw Requests Found:
# ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┓
# ┃ Request ID ┃ Repositories  ┃ Metadata State ┃ Actual State┃ Should Be ┃ Created  ┃
# ┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━┩
# │ a1b2c3d4...│ deepfreeze-001│ in_progress    │ EXPIRED     │ refrozen  │ 2025-11-03│
# │ e5f6g7h8...│ deepfreeze-002│ in_progress    │ EXPIRED     │ refrozen  │ 2025-11-03│
# │ i9j0k1l2...│ deepfreeze-003│ in_progress    │ EXPIRED     │ refrozen  │ 2025-11-03│
# ...
#
# DRY-RUN: No changes made. Run without --dry-run to apply fixes.

# Step 2: Fix the stale requests
curator_cli deepfreeze repair-metadata

# Output:
# [Same tables as above...]
#
# Thaw Request Repair Results:
#   Fixed: 7

# Now all 7 requests are marked as 'refrozen' instead of 'in_progress'
```

### Scripted Verification

```bash
# Machine-readable output for monitoring
curator_cli --dry-run deepfreeze repair-metadata --porcelain

# Output:
# TOTAL_REPOS=58
# CORRECT=10
# DISCREPANCIES=48
# ERRORS=0
# REPOS_TO_FIX:
#   deepfreeze-000004: metadata=active, actual=GLACIER
#   deepfreeze-000005: metadata=active, actual=GLACIER
#   ...

# Parse in scripts:
if curator_cli --dry-run deepfreeze repair-metadata --porcelain | grep -q "DISCREPANCIES=0"; then
    echo "✓ All metadata correct"
else
    echo "⚠ Metadata discrepancies found"
    curator_cli deepfreeze repair-metadata
fi
```

### Monthly Verification (Cron)

```bash
# /etc/cron.d/deepfreeze-verify-metadata
# Verify metadata monthly, auto-fix if needed
0 2 1 * * curator_cli deepfreeze repair-metadata >> /var/log/deepfreeze-repair.log 2>&1
```

### Pre-Upgrade Verification

```bash
# Before upgrading curator, verify metadata is correct
echo "Checking repository metadata before upgrade..."
curator_cli --dry-run deepfreeze repair-metadata

# Fix any issues
curator_cli deepfreeze repair-metadata

# Verify fix
curator_cli deepfreeze status --show-repos | grep "State"
```

### Post-Migration Verification

```bash
# After migrating data or recovering from backup
curator_cli --dry-run deepfreeze repair-metadata

# If discrepancies found, investigate before fixing
# (May indicate incomplete migration)

# Fix only if migration was successful
curator_cli deepfreeze repair-metadata
```

## Detection Logic

### S3 Storage Class Detection

```python
def _check_repo_storage_class(bucket, base_path):
    glacier_count = 0
    standard_count = 0
    total_count = 0

    # List objects (sampled for performance)
    for obj in s3.list_objects(Bucket=bucket, Prefix=base_path, MaxKeys=100):
        total_count += 1
        storage_class = obj.get('StorageClass', 'STANDARD')

        if storage_class in ['GLACIER', 'DEEP_ARCHIVE', 'GLACIER_IR']:
            glacier_count += 1
        else:
            standard_count += 1

    # Determine state
    if total_count == 0:
        return 'EMPTY'
    elif glacier_count == total_count:
        return 'GLACIER'
    elif glacier_count > 0:
        return 'MIXED'
    else:
        return 'STANDARD'
```

### Discrepancy Detection

```python
for repo in all_repos:
    # Get metadata state
    metadata_thaw_state = repo['thaw_state']

    # Check S3 storage
    actual_storage = check_repo_storage_class(repo['bucket'], repo['base_path'])

    # Compare
    expected_frozen = (metadata_thaw_state == 'frozen')
    actually_frozen = (actual_storage == 'GLACIER')

    if expected_frozen != actually_frozen:
        discrepancies.append({
            'name': repo['name'],
            'metadata_state': metadata_thaw_state,
            'actual_storage': actual_storage,
            'mounted': repo['is_mounted']
        })
```

### Repair Logic

```python
for discrepancy in discrepancies:
    repo = get_repository(discrepancy['name'])
    actual_storage = discrepancy['actual_storage']

    if actual_storage == 'GLACIER':
        # S3 is frozen, metadata should be too
        repo.reset_to_frozen()
        repo.persist(client)

    elif actual_storage == 'STANDARD':
        # S3 is not frozen
        if discrepancy['metadata_state'] == 'frozen':
            # But metadata says frozen - fix it
            repo.thaw_state = 'active'
            repo.is_thawed = False
            repo.persist(client)

    else:
        # MIXED or EMPTY - skip (edge case)
        log_warning(f"Skipping {repo.name} with {actual_storage} storage")
```

## Error Handling

### Common Issues

#### 1. S3 Access Denied

**Error**: `Failed to check S3 storage for bucket/path: Access Denied`

**Cause**: AWS credentials lack S3 read permissions

**Effect**: Repository skipped, counted as error (not fixed)

**Solutions**:
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:ListBucket",
    "s3:GetObjectAttributes",
    "s3:GetObject"
  ],
  "Resource": [
    "arn:aws:s3:::deepfreeze*",
    "arn:aws:s3:::deepfreeze*/*"
  ]
}
```

#### 2. Repository Not Found

**Error**: `Repository deepfreeze-000042 not found`

**Cause**: Repository exists in status index but has no ES document (data corruption)

**Effect**: Counted as failed, repair continues

**Action**:
- Investigate status index integrity
- May need to recreate repository metadata manually
- Check for partial cleanup or migration issues

#### 3. No Bucket/Base Path Info

**Log**: `Skipping deepfreeze-000001 - no bucket/base_path info`

**Cause**: Very old repository created before these fields were tracked

**Effect**: Skipped (cannot verify without S3 location)

**Action**:
- Manually update repository document with bucket/base_path
- Or accept that old repos cannot be verified

#### 4. Empty Repository

**Log**: `deepfreeze-000015: metadata=frozen, s3=EMPTY`

**Cause**: Repository has no objects in S3 (possibly deleted manually)

**Effect**: Shown as correct (EMPTY treated as special case)

**Action**:
- Investigate why repository is empty
- May indicate data loss or incomplete cleanup

#### 5. Mixed Storage Class

**Log**: `Skipping deepfreeze-000023 with MIXED storage`

**Cause**: Some objects in GLACIER, some in STANDARD (transition in progress)

**Effect**: Skipped (ambiguous state, cannot determine correct metadata)

**Action**:
- Wait for S3 lifecycle transition to complete
- Or manually investigate and fix
- Re-run repair after transition completes

## Best Practices

### When to Run

1. **After Upgrade** (Required)
   ```bash
   # Immediately after upgrading to version with metadata fix
   curator_cli --dry-run deepfreeze repair-metadata
   curator_cli deepfreeze repair-metadata
   ```

2. **Monthly Verification** (Recommended)
   ```bash
   # Cron: First day of month at 2 AM
   0 2 1 * * curator_cli deepfreeze repair-metadata
   ```

3. **Before Major Operations** (Best Practice)
   ```bash
   # Before large thaw operations
   curator_cli --dry-run deepfreeze repair-metadata

   # Before migrations or cluster changes
   curator_cli deepfreeze repair-metadata
   ```

4. **After Manual S3 Operations**
   ```bash
   # If you manually changed storage classes
   curator_cli deepfreeze repair-metadata
   ```

### Verification Workflow

```bash
# 1. Check current status
curator_cli deepfreeze status --show-repos

# 2. Run dry-run to see what would change
curator_cli --dry-run deepfreeze repair-metadata

# 3. Review discrepancies
# - Are they expected? (post-upgrade = yes)
# - Are they unexpected? (investigate before fixing)

# 4. Fix if appropriate
curator_cli deepfreeze repair-metadata

# 5. Verify fix
curator_cli deepfreeze status --show-repos
curator_cli --dry-run deepfreeze repair-metadata  # Should show 0 discrepancies
```

### Monitoring

1. **Log Analysis**
   ```bash
   # Check repair logs
   grep "repair_metadata" /var/log/curator.log

   # Look for patterns
   grep "Fixed:" /var/log/deepfreeze-repair.log
   ```

2. **Metrics Collection**
   ```bash
   # Extract metrics from porcelain output
   curator_cli --dry-run deepfreeze repair-metadata --porcelain | \
     grep "DISCREPANCIES=" | \
     awk -F= '{print $2}'
   ```

3. **Alerting**
   ```bash
   # Alert if discrepancies found
   DISCREPANCIES=$(curator_cli --dry-run deepfreeze repair-metadata --porcelain | grep "DISCREPANCIES=" | cut -d= -f2)

   if [ "$DISCREPANCIES" -gt 0 ]; then
       echo "WARNING: $DISCREPANCIES repositories have metadata discrepancies"
       # Send alert
   fi
   ```

### Safety Considerations

1. **Always Dry-Run First**
   - See what would change before applying
   - Verify changes match expectations
   - Catch unexpected discrepancies

2. **Investigate Unexpected Discrepancies**
   - Post-upgrade discrepancies are expected
   - Sudden discrepancies may indicate:
     - Bug recurrence
     - Manual S3 operations
     - Data corruption
     - Cluster issues

3. **Backup Before Large Repairs**
   ```bash
   # Snapshot status index before fixing many repos
   curl -X PUT "localhost:9200/_snapshot/backup/deepfreeze-status-pre-repair?wait_for_completion=true" \
     -H 'Content-Type: application/json' -d'
   {
     "indices": "deepfreeze-status",
     "include_global_state": false
   }'
   ```

4. **Verify After Repair**
   ```bash
   # Re-run dry-run to confirm 0 discrepancies
   curator_cli --dry-run deepfreeze repair-metadata | grep "discrepancies: 0"
   ```

## Comparison: Repair Metadata vs Status

| Aspect | Repair Metadata | Status |
|--------|-----------------|--------|
| **Purpose** | Fix metadata discrepancies | View current state |
| **S3 Checks** | Yes (checks storage class) | No (shows metadata only) |
| **Modifies Data** | Yes (updates metadata) | No (read-only) |
| **When to Run** | After upgrades, monthly | Anytime, frequently |
| **Speed** | Slower (S3 API calls) | Fast (ES queries only) |
| **Output** | Discrepancies and fixes | Current state |

**Use Together**:
```bash
# Before repair: See what metadata says
curator_cli deepfreeze status --show-repos

# Run repair
curator_cli deepfreeze repair-metadata

# After repair: Verify new metadata
curator_cli deepfreeze status --show-repos
```

## Performance Considerations

### Operation Speed

- **Query Phase**: 1-2 seconds (Elasticsearch query)
- **S3 Check Phase**: 1-2 seconds per repository (with sampling)
- **Repair Phase**: 0.5-1 second per repository (ES updates)

**Typical Total Time**:
- 50 repositories: 60-120 seconds (1-2 minutes)
- 100 repositories: 120-240 seconds (2-4 minutes)

### Resource Impact

- **CPU**: Low (simple comparisons)
- **Memory**: Low (processes one repository at a time)
- **Network**: Moderate (S3 API calls, Elasticsearch queries)
- **S3 API Calls**: 1 per repository (ListObjects)
- **ES Operations**: 1 query + N updates (where N = discrepancies)

### Optimization

1. **Sampling Strategy**
   - Only checks first 100 objects per repository
   - Sufficient to determine storage state
   - Avoids scanning large repositories fully

2. **Sequential Processing**
   - One repository at a time
   - Avoids overwhelming S3 API
   - Prevents rate limiting

3. **Conditional Updates**
   - Only updates repositories with discrepancies
   - Skips repositories with correct metadata

### Scheduling Considerations

- **Time**: Off-peak hours (reduces S3 API cost)
- **Frequency**: Monthly is sufficient (not time-critical)
- **Conflict Avoidance**: Don't run during rotation or major thaw operations

## Related Actions

- **Setup**: Creates initial repository metadata (repair fixes it later)
- **Rotate**: Transitions repositories (previously had bug causing desync)
- **Status**: Shows repository states (repair ensures states are accurate)
- **Thaw**: Depends on accurate frozen/active states
- **Cleanup**: Processes expired repositories (needs accurate states)

## State Transitions

### Correct State Transitions

```
S3: STANDARD → GLACIER (via rotate)
Metadata: active → frozen (via rotate)
✓ Synchronized

S3: GLACIER → STANDARD (via AWS restore)
Metadata: frozen → thawed (via thaw action)
✓ Synchronized
```

### Bug: Desynchronized State

```
S3: STANDARD → GLACIER (via rotate)
Metadata: active → active (BUG: forgot to update)
✗ Desynchronized

Repair Metadata:
S3: GLACIER (detected)
Metadata: active → frozen (corrected)
✓ Re-synchronized
```

## Dry Run Example

```bash
curator_cli --dry-run deepfreeze repair-metadata

# Output:

Metadata Repair Report (DRY-RUN)

Total repositories scanned: 58
Repositories with correct metadata: 10
Repositories with discrepancies: 48

Discrepancies Found:
┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Repository        ┃ Metadata State ┃ Actual S3 Storage ┃ Mounted ┃
┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ deepfreeze-000004 │ active         │ GLACIER           │ No      │
│ deepfreeze-000005 │ active         │ GLACIER           │ No      │
│ deepfreeze-000006 │ active         │ GLACIER           │ No      │
│ deepfreeze-000007 │ active         │ GLACIER           │ No      │
│ deepfreeze-000008 │ active         │ GLACIER           │ No      │
│ deepfreeze-000009 │ active         │ GLACIER           │ No      │
│ deepfreeze-000010 │ active         │ GLACIER           │ No      │
│ deepfreeze-000011 │ active         │ GLACIER           │ No      │
│ deepfreeze-000012 │ active         │ GLACIER           │ No      │
│ deepfreeze-000013 │ active         │ GLACIER           │ No      │
│ deepfreeze-000014 │ active         │ GLACIER           │ No      │
│ deepfreeze-000015 │ active         │ GLACIER           │ No      │
│ deepfreeze-000016 │ active         │ GLACIER           │ No      │
│ deepfreeze-000017 │ active         │ GLACIER           │ No      │
│ deepfreeze-000018 │ active         │ GLACIER           │ No      │
│ deepfreeze-000019 │ active         │ GLACIER           │ No      │
│ deepfreeze-000020 │ active         │ GLACIER           │ No      │
│ deepfreeze-000021 │ active         │ GLACIER           │ No      │
│ deepfreeze-000022 │ active         │ GLACIER           │ No      │
│ deepfreeze-000023 │ active         │ GLACIER           │ No      │
│ deepfreeze-000024 │ active         │ GLACIER           │ No      │
│ deepfreeze-000025 │ active         │ GLACIER           │ No      │
│ deepfreeze-000026 │ active         │ GLACIER           │ No      │
│ deepfreeze-000027 │ active         │ GLACIER           │ No      │
│ deepfreeze-000028 │ active         │ GLACIER           │ No      │
│ deepfreeze-000029 │ active         │ GLACIER           │ No      │
│ deepfreeze-000030 │ active         │ GLACIER           │ No      │
│ deepfreeze-000031 │ active         │ GLACIER           │ No      │
│ deepfreeze-000032 │ active         │ GLACIER           │ No      │
│ deepfreeze-000033 │ active         │ GLACIER           │ No      │
│ deepfreeze-000034 │ active         │ GLACIER           │ No      │
│ deepfreeze-000035 │ active         │ GLACIER           │ No      │
│ deepfreeze-000036 │ active         │ GLACIER           │ No      │
│ deepfreeze-000037 │ active         │ GLACIER           │ No      │
│ deepfreeze-000038 │ active         │ GLACIER           │ No      │
│ deepfreeze-000039 │ active         │ GLACIER           │ No      │
│ deepfreeze-000040 │ active         │ GLACIER           │ No      │
│ deepfreeze-000041 │ active         │ GLACIER           │ No      │
│ deepfreeze-000042 │ active         │ GLACIER           │ No      │
│ deepfreeze-000043 │ active         │ GLACIER           │ No      │
│ deepfreeze-000044 │ active         │ GLACIER           │ No      │
│ deepfreeze-000045 │ active         │ GLACIER           │ No      │
│ deepfreeze-000046 │ active         │ GLACIER           │ No      │
│ deepfreeze-000047 │ active         │ GLACIER           │ No      │
│ deepfreeze-000048 │ active         │ GLACIER           │ No      │
│ deepfreeze-000049 │ active         │ GLACIER           │ No      │
│ deepfreeze-000050 │ active         │ GLACIER           │ No      │
│ deepfreeze-000051 │ active         │ GLACIER           │ No      │
└───────────────────┴────────────────┴───────────────────┴─────────┘

DRY-RUN: No changes made. Run without --dry-run to apply fixes.
```

## Live Run Example

```bash
curator_cli deepfreeze repair-metadata

# Output:

Metadata Repair Report (LIVE)

Total repositories scanned: 58
Repositories with correct metadata: 10
Repositories with discrepancies: 48

Discrepancies Found:
[Table showing all 48 discrepancies...]

Results:
  Fixed: 48

# All 48 repositories now have correct metadata
```

## Troubleshooting

### "No repositories found in status index"

**Cause**: Deepfreeze not initialized, or status index deleted

**Solution**:
```bash
# Verify status index exists
curl -X GET "localhost:9200/deepfreeze-status"

# If missing, run setup
curator_cli deepfreeze setup
```

### Many discrepancies after upgrade

**Expected**: This is normal if upgrading from version with bug

**Action**: Run repair to fix them all

```bash
curator_cli deepfreeze repair-metadata
```

### Discrepancies appear after repair

**Unexpected**: Indicates ongoing issue

**Action**:
1. Check for concurrent rotate operations
2. Verify bug fix is applied
3. Check for manual S3 operations
4. Review recent changes to rotate code

### S3 permission errors

**Error**: Access Denied when checking storage class

**Solution**: Add required IAM permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObjectAttributes",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::deepfreeze*",
        "arn:aws:s3:::deepfreeze*/*"
      ]
    }
  ]
}
```

## Security Considerations

- **IAM Permissions**: Requires S3 read permissions
- **Elasticsearch Permissions**: Requires write access to status index
- **Audit Trail**: All repairs logged
- **No Data Modification**: Only updates metadata, doesn't touch S3 objects
- **Safe Operation**: Can be run repeatedly without harm
- **Dry-Run Available**: Preview changes before applying

## Future Enhancements

Potential improvements for future versions:

1. **Automatic Repair**
   - Option to auto-repair in status/cleanup actions
   - Periodic background verification

2. **Extended Validation**
   - Verify mounted status matches ES repository registry
   - Check snapshot metadata consistency

3. **Batch Operations**
   - Concurrent S3 checks (with rate limiting)
   - Faster processing of large deployments

4. **Reporting**
   - Export discrepancies to JSON/CSV
   - Historical tracking of repairs

5. **Integration**
   - Prometheus metrics
   - Alerting on persistent discrepancies
