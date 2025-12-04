# Refreeze Action

## Purpose

The Refreeze action is a user-initiated operation to unmount thawed repositories and return them to frozen state when you're finished accessing the data. This allows you to stop incurring AWS Standard storage costs before the automatic expiration period ends.

**Key Concept**: When you thaw data from Glacier, AWS creates temporary restored copies that exist for a specified duration (e.g., 7 days). Refreeze unmounts the repositories and cleans up searchable snapshots, but the AWS restore duration cannot be canceled early - objects will remain in Standard storage until their expiration time. However, refreezing immediately stops Elasticsearch resource usage and prevents further queries against the data.

Refreeze vs Cleanup:
- **Refreeze**: Manual, user-initiated, "I'm done with this thaw now"
- **Cleanup**: Automatic, scheduled, processes expired thaws based on `expires_at` timestamp

## Prerequisites

### System Requirements

1. **Deepfreeze Initialized**
   - Setup action must have been run successfully
   - `deepfreeze-status` index must exist with valid configuration

2. **Active Thaw Request**
   - At least one thaw request in `in_progress` status
   - OR specific thaw request ID you want to refreeze

3. **Elasticsearch Permissions**
   - `snapshot.delete_repository` - Unmount repositories
   - `indices.delete` - Delete mounted indices
   - `ilm.delete_policy` - Remove thawed ILM policies

### Data Considerations

- **Mounted Searchable Snapshots**: Will be deleted
- **Queries in Progress**: Will fail when indices are deleted
- **Data Loss**: No data is lost - snapshots remain in S3, just unmounted
- **Re-access**: Can thaw again later with new thaw request

## Effects

### What Refreeze Does

For each repository in the thaw request:

#### 1. Delete Mounted Indices

Searches for and deletes all indices mounted from the repository, including all naming variations:
- Original names (e.g., `.ds-df-test-2024.01.01-000001`)
- Partial prefix (e.g., `partial-.ds-df-test-2024.01.01-000001`)
- Restored prefix (e.g., `restored-.ds-df-test-2024.01.01-000001`)

**Important**: Queries against these indices will fail immediately after deletion.

#### 2. Unmount Repository from Elasticsearch

- Calls `DELETE /_snapshot/{repo_name}`
- Removes repository from Elasticsearch cluster
- Repository metadata remains in `deepfreeze-status` index

#### 3. Delete Per-Repository Thawed ILM Policy

- Deletes the `{repo_name}-thawed` ILM policy
- Removes policy from any indices still using it first
- Example: Deletes `deepfreeze-000010-thawed` when unmounting `deepfreeze-000010`

#### 4. Reset Repository State

- Updates repository document in `deepfreeze-status` index
- State transitions: `thawed` → `frozen` (or `thawing` → `frozen`)
- Clears `is_mounted` flag
- Clears `expires_at` timestamp
- Persists state change

#### 5. Mark Thaw Request as Refrozen

- Updates thaw request document in `deepfreeze-status` index
- Status transitions: `in_progress` → `refrozen`
- Cleanup action will remove old refrozen requests based on retention settings (default: 35 days)

### What Refreeze Does NOT Do

- **Does NOT cancel AWS Glacier restore**: Objects remain in Standard storage until `expires_at` time
- **Does NOT delete snapshots**: Snapshot data remains in S3
- **Does NOT delete S3 objects**: All data is preserved
- **Does NOT affect other thaw requests**: Only processes specified request(s)
- **Does NOT revert storage class**: AWS handles automatic reversion after duration expires

### AWS Glacier Restore Duration

**Critical Understanding**:
```
Thaw initiated: 2025-01-15 09:00 UTC
Duration: 7 days
Expires at: 2025-01-22 09:00 UTC

User refreezes: 2025-01-16 10:00 UTC (1 day later)

Result:
- ✅ Elasticsearch repositories unmounted immediately
- ✅ Searchable snapshots deleted immediately
- ✅ ILM policies removed immediately
- ❌ AWS objects remain in Standard storage until 2025-01-22 09:00
- ❌ You pay Standard storage costs until 2025-01-22 09:00

Savings:
- Elasticsearch compute (no longer processing queries)
- Elasticsearch storage (indices deleted)
- BUT: Still pay AWS Standard storage for remaining duration
```

To minimize costs, plan your thaw duration carefully rather than relying on early refreeze.

## Options

### Thaw Request Selection

#### `--thaw-request-id <uuid>`
- **Type**: String (UUID)
- **Required**: No (if omitted, prompts to refreeze all open requests)
- **Description**: Specific thaw request ID to refreeze
- **Example**: `--thaw-request-id a1b2c3d4-e5f6-7890-abcd-ef1234567890`
- **Use Case**: Refreeze specific request when you're done with that dataset

#### Bulk Mode (No `--thaw-request-id`)
- **Behavior**: Finds all thaw requests with status `in_progress`
- **Confirmation**: Prompts user to confirm (lists all requests that will be affected)
- **Use Case**: Clean up all active thaws at once
- **Safety**: Requires interactive confirmation (skipped in `--porcelain` mode)

### Output Format

#### `--porcelain`
- **Type**: Boolean flag
- **Default**: `False`
- **Description**: Machine-readable tab-separated output
- **Confirmation**: Skips interactive confirmation in bulk mode
- **Output Format**:
  - Unmounted: `UNMOUNTED\t{repo_name}`
  - Failed: `FAILED\t{repo_name}`
  - Summary: `SUMMARY\t{unmounted_count}\t{failed_count}\t{deleted_indices}\t{deleted_policies}\t{request_count}`

## Usage Examples

### Refreeze Specific Thaw Request

```bash
# After finishing your analysis
curator_cli deepfreeze refreeze \
  --thaw-request-id a1b2c3d4-e5f6-7890-abcd-ef1234567890

# Output:
# Refreeze completed for thaw request 'a1b2c3d4-...'
# Unmounted 3 repositories
# Deleted 47 indices
# Deleted 3 ILM policies
```

### Refreeze All Open Requests (Interactive)

```bash
# Refreeze all active thaws
curator_cli deepfreeze refreeze

# Output:
# WARNING: This will refreeze 2 open thaw request(s)
#
#   • a1b2c3d4-e5f6-7890-abcd-ef1234567890
#     Created: 2025-01-15T09:30:00.000Z
#     Date Range: 2025-01-01T00:00:00Z to 2025-01-07T23:59:59Z
#     Repositories: 3
#
#   • b2c3d4e5-f6a7-8901-bcde-f12345678901
#     Created: 2025-01-16T14:00:00.000Z
#     Date Range: 2025-01-10T00:00:00Z to 2025-01-15T23:59:59Z
#     Repositories: 2
#
# Do you want to proceed with refreezing all these requests? [y/N]: y
#
# Refreeze completed for 2 thaw requests
# Unmounted 5 repositories
# Deleted 89 indices
# Deleted 5 ILM policies
```

### Scripted Refreeze (Non-Interactive)

```bash
# Refreeze all without confirmation prompt
curator_cli deepfreeze refreeze --porcelain

# Output (tab-separated):
# UNMOUNTED	deepfreeze-000010
# UNMOUNTED	deepfreeze-000011
# UNMOUNTED	deepfreeze-000012
# SUMMARY	3	0	47	3	1
```

### Refreeze in Scheduled Job

```bash
#!/bin/bash
# Cron job to auto-refreeze completed analysis jobs

# Get completed analysis flag from your workflow
if [ -f /var/run/analysis-complete.flag ]; then
  # Extract thaw request ID from flag file
  THAW_ID=$(cat /var/run/analysis-complete.flag)

  # Refreeze
  curator_cli deepfreeze refreeze --thaw-request-id "$THAW_ID" --porcelain

  # Clean up flag
  rm /var/run/analysis-complete.flag
fi
```

### Dry Run (Preview Changes)

```bash
# See what would be refrozen without making changes
curator_cli deepfreeze refreeze \
  --thaw-request-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
  --dry-run

# Output:
# DRY-RUN: Would refreeze thaw request 'a1b2c3d4-...'
#
# Would process 3 repositories:
#
#   - deepfreeze-000010 (state: thawed, mounted: True)
#     Would unmount and reset to frozen
#     Would delete 15 mounted indices
#     Would delete ILM policy deepfreeze-000010-thawed
#
#   - deepfreeze-000011 (state: thawed, mounted: True)
#     Would unmount and reset to frozen
#     Would delete 18 mounted indices
#     Would delete ILM policy deepfreeze-000011-thawed
#
#   - deepfreeze-000012 (state: thawed, mounted: True)
#     Would unmount and reset to frozen
#     Would delete 14 mounted indices
#     Would delete ILM policy deepfreeze-000012-thawed
#
# DRY-RUN: Would mark thaw request 'a1b2c3d4-...' as completed
```

## Error Handling

### Common Errors and Solutions

#### 1. Thaw Request Not Found

**Error**: `Could not find thaw request 'abc123'`

**Causes**:
- Invalid request ID
- Request was already cleaned up (retention period expired)
- Typo in request ID

**Solutions**:
- List all thaw requests:
  ```bash
  curator_cli deepfreeze thaw --list-requests --include-completed
  ```
- Check status:
  ```bash
  curator_cli deepfreeze status --show-thawed
  ```

#### 2. No Open Thaw Requests

**Message**: `No open thaw requests found to refreeze`

**Cause**: All thaw requests are in `completed` or `refrozen` status

**Solutions**:
- Check status to see if already refrozen:
  ```bash
  curator_cli deepfreeze thaw --list-requests --include-completed
  ```
- No action needed if this is expected

#### 3. Repository Unmount Failed

**Error**: `Failed to unmount repository deepfreeze-000010: repository_missing_exception`

**Causes**:
- Repository was already unmounted
- Repository was manually deleted

**Effect**: Non-critical - refreeze continues and marks repository as unmounted anyway

**Action**: No user action required

#### 4. Index Deletion Failed

**Error**: `Failed to delete index partial-my-index: index_not_found_exception`

**Causes**:
- Index was already deleted
- Index name pattern didn't match

**Effect**: Non-critical - refreeze continues

**Action**: Verify with:
```bash
curl -X GET 'http://localhost:9200/_cat/indices/partial-*,restored-*?v'
```

#### 5. ILM Policy Deletion Failed

**Error**: `Failed to delete ILM policy: policy is in use`

**Cause**: Some indices still reference the policy (shouldn't happen - policy is removed from indices first)

**Solutions**:
- Manually remove policy from indices:
  ```bash
  curl -X POST 'http://localhost:9200/my-index/_ilm/remove'
  ```
- Retry refreeze

#### 6. Bulk Refreeze User Cancellation

**Message**: `Operation cancelled by user`

**Cause**: User typed 'n' or pressed Ctrl+C at confirmation prompt

**Action**: Intentional cancellation - no cleanup needed

## Best Practices

### When to Refreeze

1. **After Completing Analysis**
   - Queries finished
   - Reports generated
   - Data exported if needed

2. **When Thaw Was Overestimated**
   - Thawed 7 days but only needed 2
   - Reduce Elasticsearch resource usage
   - Free up cluster capacity

3. **Before Maintenance Windows**
   - Clean up before cluster upgrades
   - Reduce indices to manage during maintenance

4. **Cost Optimization**
   - While you can't avoid AWS Standard storage costs until expiration
   - You DO save on Elasticsearch compute and storage immediately

### Before Refreezing

1. **Verify No Active Users**
   ```bash
   # Check for active queries on thawed indices
   curl -X GET 'http://localhost:9200/_tasks?detailed=true&actions=*search'
   ```

2. **Export Critical Data**
   - If analysis results needed later, export first
   - Thawed data can be re-accessed, but requires new thaw request

3. **Document Findings**
   - Record what you learned from the data
   - Save query patterns for future thaws

4. **Check Thaw Request ID**
   ```bash
   # List active thaws
   curator_cli deepfreeze thaw --list-requests
   ```

### After Refreezing

1. **Verify Repository State**
   ```bash
   # Check repositories are frozen
   curator_cli deepfreeze status --show-repos
   ```

2. **Verify Indices Deleted**
   ```bash
   # Should return empty
   curl -X GET 'http://localhost:9200/_cat/indices/partial-*,restored-*?v'
   ```

3. **Monitor Cleanup**
   - Cleanup action will remove old refrozen requests based on retention (default: 35 days)
   - View in status until removed

4. **Plan Future Thaws**
   - Note date ranges you actually used
   - Adjust duration for next thaw based on actual usage

## Refreeze Lifecycle

### Complete Workflow

```
1. User completes analysis on thawed data
   ↓
2. User runs refreeze command
   ↓
3. For each repository in thaw request:
   ↓
   a. Find all mounted indices (original, partial-, restored- prefixes)
   ↓
   b. Delete all mounted indices
   ↓
   c. Unmount repository from Elasticsearch
   ↓
   d. Delete {repo_name}-thawed ILM policy
   ↓
   e. Update repository state: thawed → frozen
   ↓
   f. Persist state to deepfreeze-status index
   ↓
4. Mark thaw request status: in_progress → refrozen
   ↓
5. Report unmounted repos, deleted indices, deleted policies
   ↓
6. (Later) Cleanup action removes old refrozen requests after retention period
```

## State Transitions

### Repository States

```
frozen → thawing → thawed → frozen
                     ↑          ↓
                     └──────────┘
                     (refreeze)
```

### Thaw Request States

```
in_progress → completed → (deleted by cleanup after retention)
     ↓
refrozen → (deleted by cleanup after retention)
```

### Index Lifecycle

```
1. Snapshot exists in frozen repository (inaccessible)
   ↓
2. Repository thawed and mounted
   ↓
3. Index mounted as searchable snapshot (queryable)
   ↓
4. Refreeze deletes index (NOT snapshot)
   ↓
5. Snapshot remains in S3 (can mount again with new thaw)
```

## Comparison: Refreeze vs Cleanup

| Aspect | Refreeze | Cleanup |
|--------|----------|---------|
| **Trigger** | Manual, user-initiated | Automatic, scheduled |
| **Use Case** | "I'm done now" | "Time's up" (expiration) |
| **Timing** | Any time while thawed | After `expires_at` timestamp |
| **Cost Impact** | Saves ES resources, NOT AWS storage | Saves ES resources, AWS reversion automatic |
| **Request Status** | `in_progress` → `refrozen` | Marks `expired` → unmounts → `frozen` |
| **Selection** | Specific request ID or all | All expired repositories |
| **User Intent** | "Done early" | "Automatic maintenance" |

### When to Use Each

**Use Refreeze When**:
- Analysis completed before expiration
- Want to free up Elasticsearch resources immediately
- Testing/development workflows (quick iteration)
- Cost-conscious (save ES costs, even if AWS storage continues)

**Use Cleanup When**:
- Expiration time reached
- Automated operations (cron jobs)
- Hands-off maintenance
- Multiple thaws expiring at different times

## Related Actions

- **Thaw**: Create thaw requests (required before refreeze)
- **Cleanup**: Automatic expiration handling
- **Status**: View thawed repositories and active requests
- **Rotate**: Repository rotation (doesn't affect thawed repos)

## Performance Considerations

### Operation Speed

Refreeze is typically fast:
- Repository unmount: < 1 second per repo
- Index deletion: 1-5 seconds per index (depends on cluster size)
- ILM policy deletion: < 1 second per policy
- State updates: < 1 second

**Typical Total Time**: 10-60 seconds for most thaw requests

### Bulk Operations

When refreezing multiple requests:
- Processed sequentially
- Total time: (number of repos) × (seconds per repo)
- Example: 10 repos × 5 seconds = ~50 seconds

### Resource Impact

- **Minimal CPU**: Simple delete operations
- **Minimal Memory**: Small state updates
- **Network**: Elasticsearch API calls only (no S3 operations)
- **Cluster Load**: Low - safe to run during normal operations

## Security Considerations

- **Confirmation Required**: Bulk mode prompts for confirmation (safety)
- **Permissions**: Requires delete permissions (indices, repositories, ILM)
- **Audit Trail**: All operations logged
- **No Data Loss**: Snapshots remain in S3 (only mounted copies deleted)
- **Reversible**: Can thaw again with new request

## Cost Implications

### Elasticsearch Savings (Immediate)

✅ **Saves**:
- Compute: No longer processing queries
- Storage: Mounted indices deleted
- Memory: Index metadata removed

### AWS Savings (Delayed)

❌ **Does NOT Save**:
- Standard storage costs until `expires_at`
- Objects remain restored for full duration

💡 **Key Insight**: Refreeze is primarily an Elasticsearch resource optimization, not an AWS cost optimization. To minimize AWS costs, choose your thaw `--duration` carefully upfront.

### Example Cost Analysis

```
Scenario:
- Thawed: 1TB data, 7-day duration, Standard tier
- Used for: 2 days
- Refroze: Day 3

AWS Costs:
- Retrieval: $10 (paid at thaw time)
- Storage Days 1-3: $0.30 (while using)
- Storage Days 4-7: $0.40 (still paying after refreeze!)
- Total AWS: $10.70

Elasticsearch Savings (Days 4-7):
- Freed: 1TB searchable snapshot storage
- Freed: Compute for queries
- Freed: Memory for index metadata

Lesson:
- Plan duration realistically (3 days would've saved $0.40)
- Refreeze still valuable for ES resource management
```
