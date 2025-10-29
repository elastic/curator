# Rotate Action

## Purpose

The Rotate action creates a new repository and retires old ones, implementing the core lifecycle management strategy of deepfreeze. Rotation prevents any single repository from growing indefinitely, enables better cost management, and allows old data to be archived to colder storage tiers.

Rotation is typically run on a schedule (weekly, monthly, or when size thresholds are met) and orchestrates several critical operations:
1. Creates a new S3 bucket or base path
2. Registers a new Elasticsearch snapshot repository
3. Creates versioned ILM policies pointing to the new repository
4. Updates index templates to use the new policies
5. Unmounts and archives old repositories beyond the retention limit
6. Cleans up expired thawed repositories

## Prerequisites

### System Requirements

1. **Deepfreeze Initialized**
   - Setup action must have been run successfully
   - At least one repository must exist
   - `deepfreeze-status` index must exist with valid configuration

2. **ILM Policies Exist**
   - At least one ILM policy must reference the current repository
   - Policies must use `searchable_snapshot` action
   - **Critical**: Rotation REQUIRES existing ILM policies to create versioned copies
   - If no policies exist, rotation fails immediately

3. **AWS Credentials**
   - Valid AWS credentials with S3 permissions
   - Same permissions as Setup action

4. **IAM Permissions**
   - All Setup permissions (bucket/repository creation)
   - Plus: `s3:PutLifecycleConfiguration` for bucket lifecycle policies

5. **Elasticsearch Permissions**
   - `snapshot.create_repository` - Create new repository
   - `snapshot.delete_repository` - Unmount old repositories
   - `ilm.put_policy` - Create versioned ILM policies
   - `ilm.delete_policy` - Clean up old policies
   - `template.update` - Update index templates

### Planning Considerations

1. **Rotation Frequency**
   - Monthly: Common for most use cases
   - Weekly: High-volume clusters (>100GB/day)
   - Size-based: When repository exceeds threshold (e.g., 5TB)

2. **Retention Policy** (`--keep` parameter)
   - How many repositories to keep mounted
   - Older repositories beyond this limit are unmounted and frozen
   - Default: 6 repositories

3. **Naming Strategy** (from Setup)
   - `style=oneup`: Sequential (000001, 000002, ...)
   - `style=date`: Monthly (2025.01, 2025.02, ...)

4. **Repository Organization** (from Setup)
   - `rotate_by=bucket`: New bucket per rotation
   - `rotate_by=path`: Same bucket, different paths

## Effects

### Immediate Effects

#### 1. Create New Repository

**Bucket Creation** (if `rotate_by=bucket`):
- New S3 bucket: `{bucket_name_prefix}-{suffix}`
- Example: `deepfreeze-000007`

**Repository Registration**:
- New repository: `{repo_name_prefix}-{suffix}`
- Example: `deepfreeze-000007`
- Registered in Elasticsearch
- Added to `deepfreeze-status` index

#### 2. Create Versioned ILM Policies

For each ILM policy that references the old repository:

**Policy Analysis**:
- Finds all policies with `searchable_snapshot` actions
- Filters to policies referencing the current repository
- Strips old suffix (if exists) to get base policy name

**Versioned Policy Creation**:
- Creates new policy: `{base_policy_name}-{suffix}`
- Example: `my-policy` → `my-policy-000007`
- Identical phases/actions except `snapshot_repository` updated
- New repository: `deepfreeze-000007`

**Validation Check**:
- Warns if `delete_searchable_snapshot=true`
- This setting can delete snapshots when indices transition to delete phase

#### 3. Update Index Templates

**Composable Templates**:
- Scans all composable index templates
- Updates `index.lifecycle.name` setting in template
- Maps old policy name → new versioned policy name
- Example: `my-policy` → `my-policy-000007`

**Legacy Templates**:
- Same process for legacy index templates
- Ensures backward compatibility

**Effect**:
- **New indices** will use new policies (and thus new repository)
- **Existing indices** keep old policies (continue using old repository)

#### 4. Update Repository Date Ranges

For all repositories (mounted and unmounted):
- Scans snapshots in each repository
- Extracts index names and patterns
- Infers start and end dates from index names
- Updates `start` and `end` timestamps in `deepfreeze-status` index

**Purpose**: Enables thaw action to find repositories by date range

#### 5. Unmount Old Repositories

**Selection**:
- Sorts repositories by suffix (descending)
- Keeps first `keep` repositories mounted
- Unmounts remaining repositories

**For Each Unmounted Repository**:
- Skips if repository is `thawed` or `thawing` (safety check)
- Unregisters from Elasticsearch: `DELETE /_snapshot/{repo_name}`
- Pushes objects to Glacier (if not already in Glacier storage class)
- Updates repository state to `frozen` in `deepfreeze-status` index
- Cleans up associated ILM policies (see Policy Cleanup below)

#### 6. Clean Up ILM Policies for Unmounted Repositories

For each unmounted repository:
- Extracts suffix from repository name
- Finds all ILM policies with matching suffix
- For each policy:
  - Checks if safe to delete (not used by indices/datastreams/templates)
  - Deletes if safe
  - Skips if still in use

#### 7. Run Cleanup Action

After rotation completes, automatically runs Cleanup action to:
- Detect expired thawed repositories
- Unmount expired repositories
- Delete indices from expired repositories
- Clean up old thaw requests
- Clean up orphaned thawed ILM policies

### Ongoing Effects

**New Snapshots**:
- All new ILM-managed snapshots go to the new repository
- Old repositories receive no new snapshots (frozen in time)

**Index Lifecycle**:
- New indices follow new policies
- Existing indices follow old policies (eventual transition to old repos)

**Repository Growth**:
- New repository starts empty, grows over time
- Old repositories remain static in size

## Options

### Required Options

None - all options have defaults, but some scenarios require parameters.

### Retention Configuration

#### `--keep <count>`
- **Type**: Integer
- **Default**: `6`
- **Description**: Number of repositories to keep mounted (active)
- **Range**: Typically `3` to `12`
- **Calculation**: Repositories beyond this count are unmounted and frozen
- **Example**: `--keep 4` keeps the 4 most recent repositories, unmounts others

**Planning Guide**:
```
Monthly rotation, keep=6:
- Keeps last 6 months mounted
- Older than 6 months: frozen (Glacier)

Weekly rotation, keep=12:
- Keeps last 12 weeks mounted (3 months)
- Older than 3 months: frozen

Daily rotation, keep=30:
- Keeps last 30 days mounted
- Older than 30 days: frozen
```

### Date-Based Rotation (Optional)

#### `--year <YYYY>` and `--month <MM>`
- **Type**: Integer
- **Required**: Only when `style=date` (configured in Setup)
- **Default**: None (ignored when `style=oneup`)
- **Description**: Override year and month for suffix
- **Example**: `--year 2025 --month 2` creates repository with suffix `2025.02`

**Use Cases**:
- Manual rotation for specific month
- Catch-up rotation after downtime
- Testing rotation for future months

**Normal Usage** (oneup style):
```bash
# No year/month needed - suffix auto-increments
curator_cli deepfreeze rotate --keep 6
```

**Date-based Usage**:
```bash
# Explicit month specification
curator_cli deepfreeze rotate --year 2025 --month 2 --keep 6
```

## Usage Examples

### Basic Monthly Rotation

```bash
# Rotate with default retention (keep 6)
curator_cli deepfreeze rotate

# Creates:
# - New repository: deepfreeze-000007
# - Versioned policies: my-policy-000007, etc.
# - Updates templates
# - Unmounts repositories older than position 6
```

### Custom Retention

```bash
# Keep only last 3 months mounted
curator_cli deepfreeze rotate --keep 3

# More aggressive rotation:
# - Unmounts more repositories
# - Frees Elasticsearch resources
# - More data in cold storage
```

### High Retention

```bash
# Keep 12 repositories mounted (1 year for monthly rotation)
curator_cli deepfreeze rotate --keep 12

# Conservative approach:
# - More data readily accessible
# - Higher Elasticsearch resource usage
# - Less data in cold storage
```

### Date-Based Rotation

```bash
# Rotate for specific month (requires style=date in Setup)
curator_cli deepfreeze rotate --year 2025 --month 3 --keep 6

# Creates:
# - Repository: deepfreeze-2025.03
# - Policies: my-policy-2025.03
```

### Dry Run

```bash
# Preview rotation without making changes
curator_cli deepfreeze rotate --dry-run

# Output shows:
# - New repository that would be created
# - ILM policies that would be versioned
# - Templates that would be updated
# - Repositories that would be unmounted
# - Policies that would be cleaned up
```

### Scheduled Rotation (Cron)

```bash
# /etc/cron.d/deepfreeze-rotate
# Run rotation on first day of each month at 2 AM
0 2 1 * * curator_cli deepfreeze rotate --keep 6 >> /var/log/deepfreeze-rotate.log 2>&1
```

### Size-Based Rotation (Scripted)

```bash
#!/bin/bash
# Rotate when current repository exceeds 5TB

REPO_NAME="deepfreeze-000006"  # Current repo
SIZE=$(aws s3 ls --summarize --recursive s3://deepfreeze/snapshots-000006/ | grep "Total Size" | awk '{print $3}')
SIZE_TB=$((SIZE / 1024 / 1024 / 1024 / 1024))

if [ "$SIZE_TB" -gt 5 ]; then
  echo "Repository size ${SIZE_TB}TB exceeds threshold, rotating..."
  curator_cli deepfreeze rotate --keep 6
else
  echo "Repository size ${SIZE_TB}TB within limits, no rotation needed"
fi
```

## Error Handling

### Common Errors and Solutions

#### 1. No ILM Policies Found

**Error**: `No ILM policies found that reference repository deepfreeze-000006. Rotation requires existing ILM policies to create versioned copies.`

**Cause**: No ILM policies use the current repository

**Solutions**:

**Option 1**: Create ILM policy manually
```bash
curl -X PUT "http://localhost:9200/_ilm/policy/my-policy" -H 'Content-Type: application/json' -d'
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_size": "50GB",
            "max_age": "7d"
          }
        }
      },
      "frozen": {
        "min_age": "30d",
        "actions": {
          "searchable_snapshot": {
            "snapshot_repository": "deepfreeze-000006"
          }
        }
      },
      "delete": {
        "min_age": "365d",
        "actions": {
          "delete": {
            "delete_searchable_snapshot": false
          }
        }
      }
    }
  }
}'
```

**Option 2**: Re-run setup with sample policy
```bash
# This creates a sample policy
curator_cli deepfreeze setup --create-sample-ilm-policy
```

**Option 3**: Update existing policy to reference deepfreeze repo
```bash
# If you have policies that use different repositories,
# update them to use deepfreeze repository before rotating
```

#### 2. Repository Already Exists

**Error**: `Repository deepfreeze-000007 already exists`

**Causes**:
- Previous rotation failed partway through
- Manual repository creation conflict
- Clock/suffix issue with date-based rotation

**Solutions**:
```bash
# Check existing repositories
curator_cli deepfreeze status --show-repos

# If repository is stale/incomplete, delete it
curl -X DELETE 'http://localhost:9200/_snapshot/deepfreeze-000007'

# Retry rotation
curator_cli deepfreeze rotate --keep 6
```

#### 3. S3 Bucket Already Exists (bucket rotation)

**Error**: `Failed to create bucket: BucketAlreadyExists`

**Causes**:
- Previous rotation failed
- Bucket name conflict (global namespace)

**Solutions**:
```bash
# Check if bucket exists
aws s3 ls s3://deepfreeze-000007

# If empty or stale, delete it
aws s3 rb s3://deepfreeze-000007 --force

# Retry rotation
curator_cli deepfreeze rotate --keep 6
```

#### 4. Template Update Failed

**Error**: `Could not update template my-template: ...`

**Cause**: Template doesn't exist or has syntax issues

**Effect**: Non-critical - rotation continues, but template won't use new policy

**Solutions**:
```bash
# Manually update template after rotation
curl -X PUT "http://localhost:9200/_index_template/my-template" -H 'Content-Type: application/json' -d'
{
  "index_patterns": ["my-index-*"],
  "template": {
    "settings": {
      "index.lifecycle.name": "my-policy-000007"
    }
  }
}'
```

#### 5. Unable to Unmount Repository (Thawed)

**Warning**: `Skipping thawed repo deepfreeze-000004`

**Cause**: Repository is currently thawed (actively being accessed)

**Effect**: Repository NOT unmounted (safety feature)

**Action**: This is intentional - thawed repositories are protected
- Wait for thaw to complete/expire
- Or manually refreeze:
  ```bash
  curator_cli deepfreeze refreeze --thaw-request-id <id>
  ```
- Then re-run rotation

#### 6. Policy Deletion Skipped (Still in Use)

**Warning**: `Skipping policy my-policy-000003 (still in use by indices/datastreams/templates)`

**Cause**: Old versioned policy still has indices assigned

**Effect**: Non-critical - policy remains until indices are deleted

**Action**: Normal behavior - old policies cleaned up when indices eventually age out

## Best Practices

### Rotation Frequency

#### Monthly Rotation (Most Common)
- **Use Case**: Standard log retention (30-90 days hot, older in cold)
- **Keep Setting**: `--keep 6` (6 months mounted)
- **Schedule**: `0 2 1 * * *` (2 AM on 1st of month)

#### Weekly Rotation
- **Use Case**: High-volume logging (100GB+/day)
- **Keep Setting**: `--keep 12` (12 weeks ≈ 3 months)
- **Schedule**: `0 2 * * 0` (2 AM on Sundays)

#### Size-Based Rotation
- **Use Case**: Variable ingestion rates
- **Threshold**: Typically 5TB or 10TB per repository
- **Schedule**: Daily check + conditional rotation

### Retention Planning (`--keep`)

**Formula**: `keep = (days of hot data) / (rotation frequency in days)`

**Examples**:
```
Monthly rotation, want 6 months hot:
keep = 180 / 30 = 6

Weekly rotation, want 3 months hot:
keep = 90 / 7 ≈ 12-13

Daily rotation, want 30 days hot:
keep = 30 / 1 = 30
```

**Considerations**:
- **More Mounted Repos** (higher `keep`):
  - ✅ Faster queries (no thaw needed)
  - ✅ Better for frequent access patterns
  - ❌ Higher Elasticsearch resource usage
  - ❌ More storage costs

- **Fewer Mounted Repos** (lower `keep`):
  - ✅ Lower Elasticsearch resource usage
  - ✅ Lower storage costs
  - ❌ Requires thaw for older data
  - ❌ Slower access to historical data

### Before Rotation

1. **Verify ILM Policies Exist**
   ```bash
   curator_cli deepfreeze status --show-ilm
   ```

2. **Check Current Repository Usage**
   ```bash
   # Size of current repository
   aws s3 ls --summarize --recursive s3://my-bucket/snapshots-000006/

   # Snapshot count
   curl -X GET 'http://localhost:9200/_snapshot/deepfreeze-000006/_all' | jq '.snapshots | length'
   ```

3. **Review Retention Strategy**
   - How many repos currently mounted?
   - Is `--keep` setting appropriate?
   - Any thawed repos that should be refrozen first?

4. **Check for Thawed Repositories**
   ```bash
   curator_cli deepfreeze status --show-thawed
   ```
   - Thawed repos are NOT unmounted during rotation (safety)
   - Consider refreezing before rotating

5. **Dry Run**
   ```bash
   curator_cli deepfreeze rotate --dry-run --keep 6
   ```

### During Rotation

1. **Monitor Progress**
   - Rotation logs extensively
   - Watch for errors in ILM policy creation
   - Verify template updates succeed

2. **Expect Brief Impact**
   - ILM policy creation: < 1 second per policy
   - Template updates: < 1 second per template
   - Repository unmounting: < 1 second per repo
   - Total typical time: 10-30 seconds

### After Rotation

1. **Verify New Repository**
   ```bash
   curator_cli deepfreeze status --show-repos
   # Should show new repository as active (marked with *)
   ```

2. **Check Versioned Policies Created**
   ```bash
   curator_cli deepfreeze status --show-ilm
   # Should show policies with new suffix
   ```

3. **Verify Templates Updated**
   ```bash
   # List index templates
   curl -X GET 'http://localhost:9200/_index_template'

   # Check specific template
   curl -X GET 'http://localhost:9200/_index_template/my-template' | jq '.index_templates[0].index_template.template.settings."index.lifecycle.name"'
   # Should show: "my-policy-000007" (new suffix)
   ```

4. **Monitor New Snapshots**
   ```bash
   # Wait for next ILM snapshot action
   # Verify it goes to new repository
   curl -X GET 'http://localhost:9200/_snapshot/deepfreeze-000007/_all'
   ```

5. **Verify Old Repos Unmounted**
   ```bash
   curl -X GET 'http://localhost:9200/_snapshot/_all' | jq 'keys'
   # Should only show last 'keep' repositories
   ```

6. **Check Cleanup Occurred**
   - Cleanup action runs automatically after rotation
   - Check logs for expired thaw cleanup
   - Verify orphaned policies removed

## Rotation Lifecycle

### Complete Workflow

```
1. User runs rotate command
   ↓
2. Validate ILM policies exist (fail-fast if none)
   ↓
3. Generate new suffix (oneup: increment, date: from --year/--month)
   ↓
4. Create new S3 bucket (if rotate_by=bucket)
   ↓
5. Register new Elasticsearch repository
   ↓
6. Find all ILM policies referencing current repository
   ↓
7. For each policy:
   a. Strip old suffix (if exists)
   b. Create versioned policy with new suffix
   c. Update snapshot_repository to new repo
   ↓
8. Update all index templates:
   a. Scan composable templates
   b. Update ILM policy references
   c. Scan legacy templates
   d. Update ILM policy references
   ↓
9. Update date ranges for all repositories
   ↓
10. Determine repositories to unmount (beyond --keep)
   ↓
11. For each old repository to unmount:
   a. Skip if thawed (safety)
   b. Unmount from Elasticsearch
   c. Push objects to Glacier (if not already)
   d. Update state to frozen
   e. Clean up associated ILM policies
   ↓
12. Save updated settings (last_suffix)
   ↓
13. Run Cleanup action
   ↓
14. Report success
```

## State Transitions

### Repository States During Rotation

```
Active Repository (deepfreeze-000006):
- Remains mounted
- No longer receives new snapshots (after rotation)
- Eventually unmounted in future rotation (when beyond --keep)

New Repository (deepfreeze-000007):
- Created during rotation
- Becomes active (receives new snapshots)
- Marked with * in status output

Old Repositories (deepfreeze-000001 to 000005):
- Status depends on --keep value
- If within keep limit: remain mounted
- If beyond keep limit: unmounted → frozen
```

### ILM Policy Lifecycle

```
Original Policy: my-policy
   ↓
Rotation 1: my-policy-000001 created
   ↓
Rotation 2: my-policy-000002 created
   ↓
... (keep using old policies)
   ↓
When indices using my-policy-000001 deleted:
   ↓
Cleanup removes my-policy-000001 (safe to delete)
```

## Versioned ILM Policies

### Why Versioning?

**Problem**: Modifying existing policies affects all indices using them
- Existing indices would suddenly switch repositories
- Could break ongoing snapshots
- Creates race conditions

**Solution**: Create NEW versioned policies for each rotation
- Existing indices keep old policies → old repositories
- New indices get new policies → new repository
- Clean separation, no conflicts

### Policy Versioning Example

**Before Rotation** (Repository: deepfreeze-000006):
```json
{
  "my-logs-policy": {
    "policy": {
      "phases": {
        "frozen": {
          "actions": {
            "searchable_snapshot": {
              "snapshot_repository": "deepfreeze-000006"
            }
          }
        }
      }
    }
  }
}
```

**After Rotation** (Repository: deepfreeze-000007):
```json
{
  "my-logs-policy-000007": {
    "policy": {
      "phases": {
        "frozen": {
          "actions": {
            "searchable_snapshot": {
              "snapshot_repository": "deepfreeze-000007"
            }
          }
        }
      }
    }
  }
}
```

**Index Template Updated**:
```json
{
  "my-template": {
    "index_patterns": ["logs-*"],
    "template": {
      "settings": {
        "index.lifecycle.name": "my-logs-policy-000007"
      }
    }
  }
}
```

**Result**:
- New indices (logs-2025.02.01-000001) → `my-logs-policy-000007` → `deepfreeze-000007`
- Old indices (logs-2025.01.15-000023) → `my-logs-policy-000006` → `deepfreeze-000006`

## Policy Cleanup

### When Policies Are Deleted

During rotation, when unmounting old repositories, associated policies are cleaned up:

1. **Extract Suffix**: From repository name (e.g., `deepfreeze-000003` → `000003`)
2. **Find Matching Policies**: All policies ending with `-000003`
3. **Safety Check**: For each policy:
   - Check if used by indices
   - Check if used by datastreams
   - Check if referenced in templates
4. **Delete if Safe**: Only delete when no usage found
5. **Skip if In Use**: Keep policy until indices age out

**Example**:
```
Repository unmounted: deepfreeze-000003

Policies found: my-logs-policy-000003, security-policy-000003

Check my-logs-policy-000003:
- 0 indices using it (all aged out)
- 0 datastreams
- 0 templates
→ DELETE my-logs-policy-000003 ✅

Check security-policy-000003:
- 12 indices still using it
→ SKIP security-policy-000003 ⏭️
  (will be cleaned up when indices deleted)
```

## Related Actions

- **Setup**: Initialize deepfreeze (required first)
- **Status**: View repositories, ILM policies, configuration
- **Cleanup**: Automatically run after rotation
- **Thaw**: Access data from frozen repositories
- **Refreeze**: Unmount thawed repositories

## Performance Considerations

### Rotation Speed

Typical rotation times:
- New repository creation: < 5 seconds
- ILM policy versioning: < 1 second per policy
- Template updates: < 1 second per template
- Repository unmounting: < 1 second per repo
- Cleanup action: 5-30 seconds

**Total Time**: Usually 30-60 seconds for typical deployments

### Resource Impact

- **Minimal CPU**: Simple API operations
- **Minimal Memory**: Small state updates
- **Network**: Elasticsearch API calls, S3 bucket creation
- **Cluster Load**: Very low - safe to run during business hours

### Scheduling Recommendations

- **Time**: Off-peak hours (e.g., 2 AM)
- **Frequency**: Aligned with data lifecycle (usually monthly)
- **Avoid**: During heavy ingestion periods or maintenance windows

## Security Considerations

- **IAM Permissions**: Use least privilege (only required S3/ES permissions)
- **Audit Trail**: All operations logged
- **Policy Safety**: Checks prevent deletion of in-use policies
- **Thaw Protection**: Thawed repositories not unmounted automatically
- **Reversible**: Can re-mount repositories if needed (via thaw)

## Cost Implications

### S3 Costs

**New Repository**:
- Standard storage (while mounted): ~$0.023/GB/month
- Minimal cost initially (empty)

**Old Repositories**:
- Unmounted and objects pushed to Glacier
- Glacier storage: ~$0.004/GB/month (80% cheaper)
- Transition from Standard → Glacier saves costs

### Elasticsearch Costs

**Mounted Repositories**:
- Count controlled by `--keep` parameter
- More mounted = more metadata overhead
- Less mounted = lower resource usage

**Optimization**:
- Lower `--keep` = lower Elasticsearch costs
- Higher `--keep` = faster access (no thaw needed)

### Example Cost Analysis

```
Monthly rotation, keep=6, 1TB/month:

Month 1: 1TB in Standard ($23)
Month 2: 2TB in Standard ($46)
...
Month 6: 6TB in Standard ($138)
Month 7: Rotation!
  - 6TB in Standard ($138) - keep=6
  - 1TB → Glacier ($4)
Month 8:
  - 6TB in Standard ($138)
  - 2TB in Glacier ($8)
...
Steady state (after initial fill):
  - 6TB in Standard ($138)
  - Remaining in Glacier ($4/TB/month)

Annual savings vs all-Standard:
  - All in Standard: 12 months × average 6TB = $828/year
  - With rotation (keep=6): 6TB Standard + 6TB Glacier = $162 + $288 = $450/year
  - Savings: $378/year (45% reduction)
```
