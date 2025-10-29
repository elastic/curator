# Thaw Action

## Purpose

The Thaw action restores frozen repositories from AWS Glacier storage back to instant-access tiers, making their snapshot data available for querying and analysis. It handles the AWS Glacier restore process, repository mounting in Elasticsearch, and automatic index mounting from the restored snapshots.

Thaw supports three operational modes:
1. **Create Mode**: Initiate new thaw requests for a date range
2. **Check Status Mode**: Monitor and mount repositories when restoration completes
3. **List Mode**: Display all active thaw requests

## Prerequisites

### System Requirements

1. **Deepfreeze Initialized**
   - Setup action must have been run successfully
   - `deepfreeze-status` index must exist with valid configuration

2. **Frozen Repositories**
   - At least one repository in `frozen` state
   - Repository objects in S3 Glacier storage class

3. **AWS Credentials**
   - Valid AWS credentials with Glacier restore permissions
   - Credentials accessible to the Curator process

4. **IAM Permissions**
   - `s3:RestoreObject` - Initiate Glacier restore
   - `s3:GetObject` - Check restore status
   - `s3:ListBucket` - List objects in repository paths
   - `s3:GetObjectAttributes` - Query object restore status

5. **Elasticsearch Permissions**
   - `snapshot.create` - Mount repositories
   - `indices.create` - Mount indices from snapshots

### Data Requirements

For **Create Mode**:
- Know the date range of data you need
- Understand the duration you'll need access (affects AWS costs)
- Consider retrieval tier based on urgency (Standard, Expedited, Bulk)

For **Check Status Mode**:
- Have a thaw request ID from a previous create operation

## Effects

### Create Mode Effects

#### What Happens Immediately

1. **Repository Identification**
   - Searches `deepfreeze-status` index for repositories with date ranges overlapping the requested dates
   - Filters to only repositories in `frozen` state (not already thawed or thawing)

2. **Glacier Restore Initiation**
   - For each repository:
     - Lists all S3 objects in the repository path
     - Submits restore requests for each object
     - AWS begins retrieving objects from Glacier

3. **Repository State Update**
   - Repositories marked as `thawing` in `deepfreeze-status` index
   - `expires_at` timestamp set based on duration parameter
   - `thaw_state` transitions: `frozen` → `thawing`

4. **Thaw Request Creation**
   - Creates a tracking document in `deepfreeze-status` index
   - Records:
     - Unique request ID (UUID)
     - List of repositories being thawed
     - Date range requested
     - Status (`in_progress`)
     - Creation timestamp

5. **Response**
   - **Async Mode** (default): Returns immediately with request ID
   - **Sync Mode** (`--sync`): Waits for restore completion, then mounts repositories and indices

#### What Happens Over Time (Async Mode)

1. **AWS Glacier Restore** (hours to days depending on tier)
   - Objects transition from Glacier to Standard storage
   - Temporary copies created (original Glacier object remains)
   - Duration controlled by `--duration` parameter

2. **Check Status Process** (when you run `--check-status <id>`)
   - Queries S3 for restore status of all objects
   - When all objects restored:
     - Mounts repositories in Elasticsearch
     - Updates date ranges by scanning snapshots
     - Mounts indices from snapshots
     - Marks request as `completed`

### Check Status Mode Effects

1. **Status Query**
   - Retrieves thaw request from `deepfreeze-status` index
   - Gets associated repository objects

2. **S3 Restore Check**
   - For each repository not yet mounted:
     - Queries S3 for object restore status
     - Counts: total objects, restored, in progress, not restored

3. **Repository Mounting** (when restore complete)
   - Registers repository in Elasticsearch
   - Repository becomes available for snapshot operations
   - Updates `is_mounted` flag and state to `thawed`

4. **Date Range Update**
   - Scans mounted repository snapshots
   - Extracts index names and date patterns
   - Updates repository `start` and `end` timestamps

5. **Index Mounting**
   - Identifies indices within requested date range
   - Mounts as searchable snapshots
   - Adds to data streams if applicable
   - Creates per-repository thawed ILM policy

6. **Request Status Update**
   - Marks request as `completed` when all repositories mounted
   - Updates timestamps

### List Mode Effects

- Queries `deepfreeze-status` index for thaw request documents
- Displays in tabular format
- **No state changes** - read-only operation

## Options

### Create Mode Options

#### Date Range (Required)

##### `--start-date <ISO8601>`
- **Type**: ISO 8601 datetime string
- **Required**: Yes (for create mode)
- **Format**: `YYYY-MM-DDTHH:MM:SSZ`
- **Description**: Start of the date range to thaw
- **Example**: `2025-01-01T00:00:00Z`
- **Important**: Must be before or equal to `--end-date`

##### `--end-date <ISO8601>`
- **Type**: ISO 8601 datetime string
- **Required**: Yes (for create mode)
- **Format**: `YYYY-MM-DDTHH:MM:SSZ`
- **Description**: End of the date range to thaw
- **Example**: `2025-01-31T23:59:59Z`

#### Restore Configuration

##### `--duration <days>`
- **Type**: Integer
- **Default**: `7`
- **Range**: `1` to `90` (AWS S3 limit)
- **Description**: Number of days to keep objects restored from Glacier
- **AWS Billing**: You pay for Standard storage for this duration
- **After Duration**: Objects automatically revert to Glacier (no manual cleanup needed)
- **Example**: `--duration 3` keeps data accessible for 3 days

##### `--retrieval-tier <tier>`
- **Type**: String
- **Default**: `Standard`
- **Options**:
  - `Expedited` - 1-5 minutes (most expensive, limited capacity)
  - `Standard` - 3-5 hours (moderate cost)
  - `Bulk` - 5-12 hours (lowest cost, best for large datasets)
- **Description**: AWS Glacier restore speed and cost tier
- **Cost Comparison** (approximate, varies by region):
  - Expedited: $30/TB retrieval + $0.03/GB prorated storage
  - Standard: $10/TB retrieval + $0.01/GB prorated storage
  - Bulk: $2.50/TB retrieval + $0.0025/GB prorated storage
- **Recommendation**: Use `Standard` for most cases, `Bulk` for cost-sensitive large restores

#### Execution Mode

##### `--sync`
- **Type**: Boolean flag
- **Default**: `False` (async mode)
- **Description**: Wait for Glacier restore to complete, then mount everything before returning
- **Use Cases**:
  - Interactive sessions where you need immediate access
  - CI/CD pipelines that need to wait for data
- **Drawbacks**: Command blocks for hours (3-12 hours typically)
- **Async Alternative**: Return immediately with request ID, use `--check-status` later

#### Output Format

##### `--porcelain`
- **Type**: Boolean flag
- **Default**: `False`
- **Description**: Machine-readable tab-separated output
- **Use Case**: Scripting, automation
- **Output Format**:
  - Success: `REQUEST\t{request_id}\t{status}\t{created_at}\t{start_date}\t{end_date}`
  - Per repo: `REPO\t{name}\t{bucket}\t{path}\t{state}\t{mounted}\t{progress}`

### Check Status Mode Options

##### `--check-status <request_id>`
- **Type**: String (UUID) or empty string
- **Description**: Check status of a specific thaw request or all requests
- **Examples**:
  - `--check-status abc123-def456` - Check specific request
  - `--check-status ""` - Check all in-progress requests
- **Behavior**: Checks S3 status, mounts repositories/indices when ready, displays current state

### List Mode Options

##### `--list-requests`
- **Type**: Boolean flag
- **Description**: List all thaw requests
- **Default Behavior**: Shows only active requests (excludes completed and refrozen)

##### `--include-completed`
- **Type**: Boolean flag
- **Default**: `False`
- **Description**: Include completed and refrozen requests in list
- **Use Case**: Auditing, historical tracking

## Usage Examples

### Basic Thaw (Async)

```bash
# Initiate thaw for January 2025 data
curator_cli deepfreeze thaw \
  --start-date 2025-01-01T00:00:00Z \
  --end-date 2025-01-31T23:59:59Z

# Output:
# Thaw Request Initiated
# Request ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
# ...
# Check status with:
# curator_cli deepfreeze thaw --check-status a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

### Thaw with Custom Duration

```bash
# Thaw for only 1 day (minimize costs)
curator_cli deepfreeze thaw \
  --start-date 2025-01-15T00:00:00Z \
  --end-date 2025-01-15T23:59:59Z \
  --duration 1
```

### Urgent Thaw (Expedited)

```bash
# Fast restore (1-5 minutes, higher cost)
curator_cli deepfreeze thaw \
  --start-date 2025-01-20T00:00:00Z \
  --end-date 2025-01-22T23:59:59Z \
  --retrieval-tier Expedited
```

### Cost-Effective Large Thaw (Bulk)

```bash
# Restore large dataset over 5-12 hours at lowest cost
curator_cli deepfreeze thaw \
  --start-date 2024-12-01T00:00:00Z \
  --end-date 2024-12-31T23:59:59Z \
  --retrieval-tier Bulk \
  --duration 3
```

### Synchronous Thaw (Wait for Completion)

```bash
# Block until data is fully accessible
curator_cli deepfreeze thaw \
  --start-date 2025-01-10T00:00:00Z \
  --end-date 2025-01-12T23:59:59Z \
  --sync

# Process continues automatically through all phases:
# Phase 1: Finding Repositories
# Phase 2: Initiating Glacier Restore
# Phase 3: Waiting for Glacier Restoration (3-5 hours typically)
# Phase 4: Mounting Repositories
# Phase 5: Updating Repository Metadata
# Phase 6: Mounting Indices
```

### Check Thaw Status

```bash
# Check specific request
curator_cli deepfreeze thaw \
  --check-status a1b2c3d4-e5f6-7890-abcd-ef1234567890

# Output shows:
# - Restore progress (e.g., "125/500 objects restored")
# - Repository mount status
# - When ready: automatically mounts repos and indices
```

### Check All Active Thaws

```bash
# Check and mount all in-progress requests
curator_cli deepfreeze thaw --check-status ""

# Useful for scheduled cron jobs to poll all pending thaws
```

### List Thaw Requests

```bash
# Show active requests only
curator_cli deepfreeze thaw --list-requests

# Show all requests (including completed)
curator_cli deepfreeze thaw --list-requests --include-completed
```

### Scripting Example

```bash
#!/bin/bash
# Script to thaw data and wait for completion

# Initiate thaw
REQUEST_ID=$(curator_cli deepfreeze thaw \
  --start-date 2025-01-01T00:00:00Z \
  --end-date 2025-01-07T23:59:59Z \
  --porcelain | awk -F'\t' '/^REQUEST/ {print $2; exit}')

echo "Thaw request created: $REQUEST_ID"

# Poll until complete
while true; do
  STATUS=$(curator_cli deepfreeze thaw --check-status "$REQUEST_ID" --porcelain \
    | awk -F'\t' '/^REQUEST/ {print $3}')

  echo "Current status: $STATUS"

  if [ "$STATUS" = "completed" ]; then
    echo "Thaw complete!"
    break
  fi

  sleep 300  # Check every 5 minutes
done
```

## Error Handling

### Common Errors and Solutions

#### 1. No Repositories Found

**Error**: `No repositories found for date range`

**Causes**:
- Date range doesn't overlap with any repository's data range
- All matching repositories are already thawed

**Solutions**:
- Check available repositories:
  ```bash
  curator_cli deepfreeze status --show-repos
  ```
- Verify date ranges in repository metadata
- Use broader date range
- Check if repositories are already thawed:
  ```bash
  curator_cli deepfreeze status --show-thawed
  ```

#### 2. Glacier Restore Permission Denied

**Error**: `Failed to thaw repository: Access Denied (S3)`

**Cause**: AWS credentials lack `s3:RestoreObject` permission

**Solution**: Update IAM policy:
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:RestoreObject",
    "s3:GetObject",
    "s3:ListBucket",
    "s3:GetObjectAttributes"
  ],
  "Resource": [
    "arn:aws:s3:::your-bucket-prefix*/*"
  ]
}
```

#### 3. Expedited Retrieval Capacity Exceeded

**Error**: `Failed to restore: InsufficientCapacityException`

**Cause**: Expedited tier has limited capacity and may be unavailable

**Solutions**:
- Use `Standard` tier instead:
  ```bash
  --retrieval-tier Standard
  ```
- Purchase provisioned capacity (AWS feature)
- Retry Expedited request later

#### 4. Invalid Date Format

**Error**: `Invalid start_date: ... Expected ISO 8601 format`

**Cause**: Date not in ISO 8601 format

**Solutions**:
- Use correct format: `YYYY-MM-DDTHH:MM:SSZ`
- Include timezone (use `Z` for UTC)
- Examples:
  - ✅ `2025-01-15T00:00:00Z`
  - ❌ `2025-01-15` (missing time and timezone)
  - ❌ `01/15/2025` (wrong format)

#### 5. Repository Mount Failure

**Error**: `Failed to mount repository: repository already exists`

**Cause**: Repository name conflicts with existing repository

**Solutions**:
- Delete conflicting repository if it's stale:
  ```bash
  curl -X DELETE 'http://localhost:9200/_snapshot/conflicting-repo'
  ```
- Check repository status:
  ```bash
  curator_cli deepfreeze status --show-repos
  ```

#### 6. Index Mount Failure

**Error**: `Failed to mount index: searchable snapshot already exists`

**Cause**: Index with same name already exists in cluster

**Solutions**:
- Delete existing searchable snapshot if it's stale:
  ```bash
  curator_cli DELETE index --name partial-my-index-name
  ```
- Check mounted indices:
  ```bash
  curl -X GET 'http://localhost:9200/_cat/indices/partial-*'
  ```

## Best Practices

### Before Thawing

1. **Plan Your Date Range Carefully**
   - Thaw only the data you need (minimizes AWS costs)
   - Consider query patterns (daily, weekly, monthly analysis)
   - Account for timezone differences

2. **Choose Appropriate Duration**
   - Minimum: 1 day
   - Typical: 3-7 days for analysis projects
   - Maximum: 90 days (AWS limit)
   - Remember: You pay for Standard storage during this period

3. **Select Right Retrieval Tier**
   - **Expedited**: Emergency access, incident investigation
   - **Standard**: Regular analysis, reports (most common)
   - **Bulk**: Large-scale data mining, cost-sensitive operations

4. **Estimate Costs**
   - Use AWS pricing calculator
   - Factor in: retrieval fees + prorated storage for duration
   - Example (us-east-1, 1TB, 7 days Standard):
     - Retrieval: ~$10
     - Storage: ~$0.70 (7/30 × $3/TB/month)
     - Total: ~$10.70

### During Thaw

1. **Async Mode Recommended**
   - Don't use `--sync` for production workflows
   - Set up monitoring instead:
     ```bash
     # Cron job every 15 minutes
     */15 * * * * curator_cli deepfreeze thaw --check-status ""
     ```

2. **Monitor Progress**
   - Use `--check-status` periodically
   - Check CloudWatch for S3 metrics
   - Review Elasticsearch logs for mount errors

3. **Handle Long-Running Operations**
   - Standard tier: 3-5 hours typical
   - Bulk tier: 5-12 hours typical
   - Plan queries accordingly

### After Thaw

1. **Verify Data Accessibility**
   ```bash
   # Check repositories mounted
   curator_cli deepfreeze status --show-thawed

   # Query data
   curl -X GET 'http://localhost:9200/my-index-*/_search?size=0'
   ```

2. **Use Refreeze When Done**
   - Don't wait for expiration if finished early
   - Saves AWS costs:
     ```bash
     curator_cli deepfreeze refreeze --thaw-request-id <id>
     ```

3. **Monitor Duration Expiration**
   - Objects auto-revert to Glacier after duration
   - Indices will fail to query after expiration
   - Use `status` command to track expiry times

## Thaw Lifecycle

### Complete Workflow

```
1. User Initiates Thaw
   ↓
2. Curator finds matching frozen repositories
   ↓
3. Curator submits Glacier restore for all objects
   ↓
4. Repository state: frozen → thawing
   ↓
5. AWS Glacier begins restore (hours)
   ↓
6. User runs --check-status periodically
   ↓
7. When complete, Curator mounts repositories
   ↓
8. Curator updates date ranges
   ↓
9. Curator mounts indices as searchable snapshots
   ↓
10. Repository state: thawing → thawed
   ↓
11. Data is queryable
   ↓
12. User analyzes data
   ↓
13. User runs refreeze OR waits for expiration
   ↓
14. Repository unmounted
   ↓
15. Repository state: thawed → frozen
   ↓
16. Objects revert to Glacier (automatic)
```

## Performance Considerations

### Factors Affecting Speed

1. **Retrieval Tier**
   - Expedited: 1-5 minutes
   - Standard: 3-5 hours
   - Bulk: 5-12 hours

2. **Data Volume**
   - More objects = longer restore
   - Parallel restore of multiple objects
   - S3 throttling may occur for very large restores

3. **Network Bandwidth**
   - Repository mounting requires metadata transfer
   - Index mounting pulls snapshot data
   - Ensure adequate bandwidth between ES and S3

### Optimization Tips

1. **Use Bulk Tier for Large Datasets**
   - Better throughput for >1TB
   - Significantly cheaper
   - Plan ahead (5-12 hour window)

2. **Thaw Repositories Incrementally**
   - Don't thaw entire year at once
   - Thaw week or month at a time
   - Reduces S3 API load and costs

3. **Check Status Efficiently**
   - Use cron jobs, not continuous polling
   - 15-30 minute intervals for Standard tier
   - 1-2 hour intervals for Bulk tier

## Related Actions

- **Setup**: Initialize deepfreeze (required first)
- **Rotate**: Create new repositories (affects available date ranges)
- **Refreeze**: Manually unmount thawed repositories before expiration
- **Cleanup**: Automatic expiration handling (runs on schedule)
- **Status**: View repository states and thaw progress

## AWS Costs

### Understanding Glacier Restore Pricing

Glacier restore has two cost components:

1. **Retrieval Fee** (one-time, per object)
   - Based on retrieval tier and data volume
   - Standard: ~$10/TB
   - Bulk: ~$2.50/TB
   - Expedited: ~$30/TB

2. **Prorated Standard Storage** (duration-based)
   - Objects temporarily in Standard tier for `duration` days
   - ~$0.023/GB/month in us-east-1
   - Prorated: `(duration / 30) × monthly_rate × size`
   - Example: 7 days, 100GB: `(7/30) × $0.023 × 100 = $0.54`

### Cost Optimization

1. **Minimize Duration**: Use shortest duration that meets your needs
2. **Use Bulk Tier**: 75% cheaper retrieval than Standard
3. **Thaw Selectively**: Only restore repositories you'll actually query
4. **Refreeze Early**: Don't pay for unused days
