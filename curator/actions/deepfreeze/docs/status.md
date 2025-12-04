# Status Action

## Purpose

The Status action provides comprehensive visibility into the current state of the deepfreeze system. It displays repositories, thawed repositories, S3 buckets, ILM policies, and configuration settings in an organized, easy-to-read format.

Status is a read-only action that makes no changes to the system. It's your primary tool for:
- Monitoring repository states and health
- Tracking thawed repositories and their expiration
- Verifying ILM policy configurations
- Auditing system configuration
- Troubleshooting issues

## Prerequisites

### System Requirements

1. **Deepfreeze Initialized**
   - Setup action must have been run successfully
   - `deepfreeze-status` index must exist

2. **Elasticsearch Access**
   - Read access to `deepfreeze-status` index
   - Read access to `_snapshot` API
   - Read access to `_ilm` API

### No Prerequisites for Data

- Works even if no repositories exist
- Works even if no thaw requests exist
- Always shows configuration (if deepfreeze is initialized)

## Effects

### What Status Does

**Read Operations Only**:
- Queries `deepfreeze-status` index for configuration and repository metadata
- Queries Elasticsearch snapshot API for repository information
- Queries Elasticsearch ILM API for policy information
- Queries cluster API for cluster name

**Display Operations**:
- Formats data into rich tables (default) or tab-separated values (`--porcelain`)
- Filters sections based on flags (or shows all if no flags)
- Applies `--limit` to restrict number of items shown

**No State Changes**:
- Does NOT modify any data
- Does NOT create or delete resources
- Does NOT affect performance (lightweight read operations)
- Safe to run at any time, as frequently as needed

## Options

### Section Filters

By default (no flags), Status shows **all sections**. Use these flags to show specific sections only:

#### `--show-repos`
- **Type**: Boolean flag
- **Description**: Show repositories section
- **Displays**:
  - Repository name (current active marked with *)
  - Thaw state (frozen, thawing, thawed, expired)
  - Mount status (yes/no)
  - Snapshot count (if mounted)
  - Date range (start and end)

#### `--show-thawed`
- **Type**: Boolean flag
- **Description**: Show only thawed and thawing repositories
- **Displays**:
  - Same columns as `--show-repos`
  - Plus: Expiration timestamp
  - Filters to only repos in `thawing`, `thawed`, or `expired` states

#### `--show-buckets`
- **Type**: Boolean flag
- **Description**: Show S3 buckets section
- **Displays**:
  - Provider (aws, etc.)
  - Bucket name (current active marked with *)
  - Base path within bucket

#### `--show-ilm`
- **Type**: Boolean flag
- **Description**: Show ILM policies section
- **Displays**:
  - Policy name
  - Repository it references (current active marked with *)
  - Number of indices using policy
  - Number of datastreams using policy

#### `--show-config`
- **Type**: Boolean flag
- **Description**: Show configuration section
- **Displays**:
  - Repo name prefix
  - Bucket name prefix
  - Base path prefix
  - Canned ACL
  - Storage class
  - Provider
  - Rotation strategy (bucket/path)
  - Naming style (oneup/date)
  - Last used suffix
  - Cluster name

### Display Options

#### `--limit <count>`
- **Type**: Integer
- **Default**: None (show all)
- **Description**: Limit number of items shown in repositories and buckets sections
- **Behavior**: Shows last N items (most recent)
- **Example**: `--limit 5` shows only the 5 most recent repositories
- **Use Case**: Large deployments with many repositories

#### `--porcelain`
- **Type**: Boolean flag
- **Default**: `False`
- **Description**: Machine-readable tab-separated output
- **Use Case**: Scripting, automation, parsing
- **Effect**: Disables rich formatting, outputs raw tab-delimited data

## Usage Examples

### Show Everything (Default)

```bash
curator_cli deepfreeze status

# Displays all sections:
# - Thawed Repositories (if any)
# - Repositories
# - Buckets
# - ILM Policies
# - Configuration
```

### Show Only Repositories

```bash
curator_cli deepfreeze status --show-repos

# Output (example):
# ┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┓
# ┃ Repository         ┃ State  ┃ Mounted  ┃ Snapshots ┃ Start             ┃ End               ┃
# ┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━┩
# │ deepfreeze-000001  │ frozen │ no       │ --        │ 2024-01-01...     │ 2024-01-31...     │
# │ deepfreeze-000002  │ frozen │ no       │ --        │ 2024-02-01...     │ 2024-02-28...     │
# │ deepfreeze-000003  │ frozen │ yes      │ 127       │ 2024-03-01...     │ 2024-03-31...     │
# │ deepfreeze-000004  │ frozen │ yes      │ 145       │ 2024-04-01...     │ 2024-04-30...     │
# │ deepfreeze-000005  │ frozen │ yes      │ 198       │ 2024-05-01...     │ 2024-05-31...     │
# │ deepfreeze-000006* │ frozen │ yes      │ 52        │ 2025-01-01...     │ 2025-01-15...     │
# └────────────────────┴────────┴──────────┴───────────┴───────────────────┴───────────────────┘
#
# * = current active repository
```

### Show Only Thawed Repositories

```bash
curator_cli deepfreeze status --show-thawed

# Shows only repositories currently being accessed (thawing or thawed)
# Includes expiration timestamp

# If no thawed repos, section is not displayed
```

### Show Only Configuration

```bash
curator_cli deepfreeze status --show-config

# Output (example):
# ┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
# ┃ Setting            ┃ Value                       ┃
# ┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
# │ Repo Prefix        │ deepfreeze                  │
# │ Bucket Prefix      │ deepfreeze                  │
# │ Base Path Prefix   │ snapshots                   │
# │ Canned ACL         │ private                     │
# │ Storage Class      │ intelligent_tiering         │
# │ Provider           │ aws                         │
# │ Rotate By          │ path                        │
# │ Style              │ oneup                       │
# │ Last Suffix        │ 000006                      │
# │ Cluster Name       │ my-production-cluster       │
# └────────────────────┴─────────────────────────────┘
```

### Show Multiple Sections

```bash
# Show repos and config only
curator_cli deepfreeze status --show-repos --show-config

# Show ILM policies and buckets
curator_cli deepfreeze status --show-ilm --show-buckets
```

### Limit Output

```bash
# Show only last 5 repositories
curator_cli deepfreeze status --show-repos --limit 5

# Useful for clusters with many repositories

# Output:
# Repositories (showing last 5 of 24)
# [table with 5 most recent repos]
```

### Scripting with Porcelain Mode

```bash
# Get configuration values
curator_cli deepfreeze status --show-config --porcelain

# Output (tab-separated):
# Repo Prefix	deepfreeze
# Bucket Prefix	deepfreeze
# Base Path Prefix	snapshots
# Canned ACL	private
# Storage Class	intelligent_tiering
# Provider	aws
# Rotate By	path
# Style	oneup
# Last Suffix	000006
# Cluster Name	my-production-cluster

# Parse in script:
REPO_PREFIX=$(curator_cli deepfreeze status --show-config --porcelain | awk -F'\t' '/^Repo Prefix/ {print $2}')
echo "Repository prefix: $REPO_PREFIX"
```

### Monitor Thawed Repositories

```bash
#!/bin/bash
# Check for thawed repositories and alert if expiring soon

curator_cli deepfreeze status --show-thawed --porcelain | while IFS=$'\t' read -r name state mounted count expires start end; do
  # Skip header
  if [ "$name" = "Repository" ]; then
    continue
  fi

  # Parse expiration time
  if [ "$expires" != "N/A" ]; then
    expire_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${expires%.*}" "+%s" 2>/dev/null)
    now_epoch=$(date "+%s")
    hours_left=$(( (expire_epoch - now_epoch) / 3600 ))

    if [ "$hours_left" -lt 12 ]; then
      echo "WARNING: Repository $name expires in $hours_left hours!"
    fi
  fi
done
```

### Audit ILM Policy Usage

```bash
# Find ILM policies with no indices
curator_cli deepfreeze status --show-ilm --porcelain | awk -F'\t' '$3 == "0" && $4 == "0" {print "Unused policy: " $1}'
```

## Display Sections

### Thawed Repositories Section

**When Shown**: Automatically when any repositories are in `thawing`, `thawed`, or `expired` states

**Columns**:
- **Repository**: Repository name
- **State**: Current thaw state (`thawing`, `thawed`, `expired`)
- **Mounted**: Whether repository is mounted in Elasticsearch (`yes`/`no`)
- **Snapshots**: Number of snapshots (if mounted, else `--`)
- **Expires**: When the AWS Glacier restore expires (ISO 8601 timestamp or `N/A`)
- **Start**: Start of date range covered by repository
- **End**: End of date range covered by repository

**Purpose**: Quick view of actively thawed data and expiration tracking

### Repositories Section

**Columns**:
- **Repository**: Repository name (active repository marked with `*`)
- **State**: Thaw state (`frozen`, `thawing`, `thawed`, `expired`)
- **Mounted**: Whether repository is mounted (`yes`/`no`)
- **Snapshots**: Number of snapshots (if mounted, else `--`)
- **Start**: Start of date range
- **End**: End of date range

**Sorting**: By repository name (typically chronological due to suffix)

**Active Indicator**: Current active repository (from `last_suffix`) marked with `*`

### Buckets Section

**Columns**:
- **Provider**: Cloud provider (`aws`, etc.)
- **Bucket**: S3 bucket name (current active marked with `*`)
- **Base_path**: Path within bucket where snapshots are stored

**Unique Entries**: Shows unique bucket/base_path combinations

**Active Indicator**: Current active bucket/path marked with `*`

### ILM Policies Section

**Columns**:
- **Policy**: ILM policy name
- **Repository**: Repository referenced in `searchable_snapshot` action (current active marked with `*`)
- **Indices**: Number of indices currently using this policy
- **Datastreams**: Number of datastreams using this policy

**Filtering**: Only shows policies that:
- Have a `searchable_snapshot` action
- Reference a repository matching the deepfreeze prefix

**Active Indicator**: Policies referencing current active repository marked with `*`

### Configuration Section

**Settings Displayed**:
- **Repo Prefix**: Repository naming prefix
- **Bucket Prefix**: S3 bucket naming prefix
- **Base Path Prefix**: S3 path prefix
- **Canned ACL**: S3 bucket ACL setting
- **Storage Class**: S3 storage class
- **Provider**: Cloud provider
- **Rotate By**: Rotation strategy (`bucket` or `path`)
- **Style**: Suffix style (`oneup` or `date`)
- **Last Suffix**: Most recently used suffix
- **Cluster Name**: Elasticsearch cluster name

## Interpreting Status Output

### Repository States

#### `frozen`
- **Meaning**: Repository exists but snapshots are in Glacier storage
- **Accessible**: No (requires thaw)
- **Mounted**: Typically `no` (unless recently unmounted)
- **Action**: Run `thaw` to restore access

#### `thawing`
- **Meaning**: AWS Glacier restore in progress
- **Accessible**: Not yet (waiting for restore)
- **Mounted**: `no` (not mounted until restore complete)
- **Action**: Wait for restore or check status with `thaw --check-status`

#### `thawed`
- **Meaning**: Restored from Glacier, accessible
- **Accessible**: Yes
- **Mounted**: `yes`
- **Expires**: Shows when restore expires
- **Action**: Query data, or refreeze when done

#### `expired`
- **Meaning**: Restore expired, should be cleaned up
- **Accessible**: No (AWS reverted to Glacier)
- **Mounted**: May be `yes` (cleanup will unmount)
- **Action**: Cleanup action will handle automatically

### Mount Status

#### `yes`
- Repository is registered in Elasticsearch
- Snapshots are queryable
- Can mount searchable snapshots
- Date range should be populated

#### `no`
- Repository is not registered
- Snapshots are not accessible
- Requires thaw (if frozen) or mount (if thawed)

### Snapshot Count

#### Number (e.g., `127`)
- Repository is mounted
- Contains this many snapshots
- Snapshots are available for restore/mount

#### `--`
- Repository is not mounted
- Cannot determine snapshot count
- Need to mount first (or thaw if frozen)

### Date Ranges

#### ISO 8601 Timestamps
- Shows start and end dates for data in repository
- Populated by scanning snapshot index names
- Used by thaw action to find repositories by date

#### `N/A`
- Date range not yet determined
- Repository may be empty
- Or dates not parsed from snapshot names

## Use Cases

### Daily Monitoring

```bash
# Quick health check
curator_cli deepfreeze status --show-thawed --show-repos --limit 3

# Shows:
# - Any thawed repos and their expiration
# - Last 3 repositories (current state)
```

### Pre-Rotation Audit

```bash
# Before rotation, check configuration
curator_cli deepfreeze status --show-config --show-ilm

# Verify:
# - Last suffix (to predict next)
# - ILM policies exist and reference current repo
```

### Troubleshooting Thaw Issues

```bash
# Check repository state
curator_cli deepfreeze status --show-repos --porcelain | grep "thawing\|thawed"

# If no output, no thaw in progress
# If output shows state, check mount status
```

### Capacity Planning

```bash
# Count total snapshots across all mounted repos
curator_cli deepfreeze status --show-repos --porcelain | awk -F'\t' 'BEGIN {sum=0} $4 ~ /^[0-9]+$/ {sum+=$4} END {print "Total snapshots: " sum}'
```

### Audit Unused ILM Policies

```bash
# Find policies with no usage
curator_cli deepfreeze status --show-ilm --porcelain | awk -F'\t' '$3 == "0" && $4 == "0" && NR > 1 {print $1}' > unused_policies.txt
```

## Error Handling

### Common Issues

#### 1. No Output / Empty Sections

**Cause**: Section has no data

**Examples**:
- No thawed repos → Thawed Repositories section not shown
- No ILM policies referencing deepfreeze repos → ILM section empty

**Action**: This is normal - status only shows what exists

#### 2. Repository Shows Mounted but Snapshot Count is `--`

**Cause**: Repository mount status desync

**Solutions**:
```bash
# Check actual mount status
curl -X GET 'http://localhost:9200/_snapshot/_all'

# If repo not in list, state is stale
# Run status again (it may auto-correct on read)
```

#### 3. Date Ranges Show `N/A`

**Cause**: Repository date range not yet scanned

**Solutions**:
- Run rotation (updates all date ranges)
- Or wait for next rotation
- Date ranges are not critical for most operations

#### 4. "Settings not found" Error

**Error**: Status fails with settings error

**Cause**: Deepfreeze not initialized

**Solution**:
```bash
curator_cli deepfreeze setup
```

## Best Practices

### Regular Monitoring

1. **Daily Quick Check**
   ```bash
   curator_cli deepfreeze status --show-thawed
   ```
   - Track active thaws
   - Monitor expirations

2. **Weekly Full Status**
   ```bash
   curator_cli deepfreeze status > weekly_status.txt
   ```
   - Full audit
   - Compare week-to-week changes

3. **Pre/Post Operation Verification**
   - Before rotation: Check configuration
   - After rotation: Verify new repo created
   - Before thaw: Check available repos/dates
   - After refreeze: Verify unmounted

### Automation

1. **Status Dashboard**
   ```bash
   # Cron job: Every hour
   curator_cli deepfreeze status --porcelain > /var/www/html/deepfreeze_status.txt
   ```
   - Parse in dashboard/monitoring tool
   - Graph repository counts over time
   - Alert on approaching expirations

2. **Alerting on Expiration**
   ```bash
   # Check for repos expiring in < 24 hours
   # Send alert if found
   ```

3. **Capacity Metrics**
   ```bash
   # Track snapshot growth
   # Predict when to rotate
   ```

## Related Actions

- **Setup**: Initialize deepfreeze (required first)
- **Rotate**: Creates new repositories (status shows them)
- **Thaw**: Changes repository states (status tracks them)
- **Refreeze**: Unmounts repositories (status reflects changes)
- **Cleanup**: Cleans up expired repos (status shows results)

## Performance Considerations

### Lightweight Operation

- **Read-only**: No writes or modifications
- **Fast**: Typically < 1 second
- **Safe**: Run as frequently as needed
- **No Impact**: Does not affect cluster performance

### Limits for Large Deployments

- Use `--limit` for clusters with 50+ repositories
- Reduces output size
- Speeds up display rendering
- Doesn't affect data fetching (already fast)

## Security Considerations

- **Read Permissions Only**: No destructive operations possible
- **Information Disclosure**: Shows repository and bucket names (ensure logs are secured)
- **Cluster Info**: Displays cluster name (informational only)

## Output Formats

### Rich Format (Default)

- **Visual**: Tables with borders, colors, formatting
- **Human-Readable**: Designed for terminal viewing
- **Interactive**: Clear section headers and spacing

### Porcelain Format (`--porcelain`)

- **Machine-Readable**: Tab-separated values
- **Parseable**: Easy to process with `awk`, `cut`, scripting
- **Stable**: Column order won't change (safe for automation)
- **No Formatting**: No colors, borders, or decoration
