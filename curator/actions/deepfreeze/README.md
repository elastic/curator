# Deepfreeze Module

**Intelligent lifecycle management for Elasticsearch snapshot repositories with AWS Glacier integration**

## Overview

Deepfreeze is a snapshot repository lifecycle management system that works alongside Elasticsearch ILM (Index Lifecycle Management) to enable cost-effective long-term data retention. While ILM manages index lifecycles and creates searchable snapshots, deepfreeze manages the snapshot repositories themselves—allowing you to preserve snapshots even after indices are deleted, and to archive entire repositories to AWS S3 Glacier for minimal storage costs.

### Key Features

- **Repository Lifecycle Management**: Automated rotation and retirement of snapshot repositories
- **Snapshot Preservation**: Keep snapshots after indices are deleted, not the default ILM behavior
- **Glacier Archival**: Automatically move old repository storage to low-cost Glacier tiers
- **On-Demand Restoration**: Retrieve frozen repositories from Glacier when historical data is needed
- **ILM Integration**: Works seamlessly with ILM policies and searchable snapshots
- **Automated Cleanup**: Automatic unmounting and cleanup of restored repositories after use
- **Cost Optimization**: Reduce long-term storage costs by up to 95%

### The Problem Deepfreeze Solves

By default, when ILM policies delete indices in the delete phase, they can also delete the backing snapshots. This means your historical data is gone forever. Deepfreeze solves this by:

1. **Preserving snapshots** after index deletion
2. **Rotating repositories** to prevent unlimited growth
3. **Archiving to Glacier** when all indices from a repository have been deleted
4. **Restoring on-demand** when you need to access historical data
5. **Managing cleanup** to prevent resource waste

### How It Works

**Normal ILM Flow** (without deepfreeze):
```
Hot Data → Searchable Snapshot → Delete Phase → Index AND Snapshot deleted
```

**ILM + Deepfreeze Flow**:
```
Data → Searchable Snapshot → Delete Phase → Index deleted, Snapshot preserved
                                                    ↓
                                        Repository archived to Glacier
                                                    ↓
                                        Restore on-demand when needed
```

## Architecture

### Components

- **Status Index** (`deepfreeze-status`): Central metadata store tracking repositories, configuration, and thaw requests
- **S3 Repositories**: Elasticsearch snapshot repositories backed by S3 buckets
- **Repository States**: `active`, `frozen`, `thawing`, `thawed`, `expired`
- **Versioned ILM Policies**: Policies that reference specific repositories for searchable snapshots

### Repository Lifecycle

```
┌──────────────┐
│    Setup     │ Creates first repository and status tracking
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    Active    │ Current repository receiving new snapshots from ILM
└──────┬───────┘
       │
       ▼  Rotate (scheduled - creates new active repository)
┌──────────────┐
│ Deep Frozen  │ Old repository archived to Glacier, unmounted
└──────┬───────┘ (all indices deleted, snapshots preserved in cold storage)
       │
       ▼  Thaw (on-demand when historical data needed)
┌──────────────┐
│   Thawing    │ Glacier restore in progress
└──────┬───────┘
       │
       ▼  Automatic when restore completes
┌──────────────┐
│    Thawed    │ Repository mounted, snapshots accessible for queries
└──────┬───────┘
       │
       ▼  Cleanup (automatic after expiration) or Refreeze (manual)
┌──────────────┐
│ Deep Frozen  │ Unmounted, back to Glacier cold storage
└──────────────┘
```

## Quick Start

### Initial Setup

```bash
# 1. Initialize deepfreeze (one-time)
curator_cli deepfreeze setup \
  --repo-name-prefix deepfreeze \
  --bucket-name-prefix my-snapshots \
  --base-path-prefix snapshots \
  --rotate-by bucket \
  --style oneup \
  --storage-class intelligent_tiering

# 2. Verify configuration
curator_cli deepfreeze status
```

### Daily Operations

```bash
# Monitor repository state
curator_cli deepfreeze status --show-repos

# Rotate to new repository (typically monthly via cron)
# This creates a new active repository and archives old ones to Glacier
curator_cli deepfreeze rotate --keep 6

# Check for expired thawed repositories
curator_cli deepfreeze cleanup
```

### Data Recovery

```bash
# Restore repositories containing data from a date range
curator_cli deepfreeze thaw --start-date 2024-01-01 --end-date 2024-01-31 --duration 7

# Check thaw progress
curator_cli deepfreeze thaw --check-status <request-id>

# Manually refreeze when finished analyzing data
curator_cli deepfreeze refreeze --thaw-request-id <request-id>
```

## Actions

### Core Lifecycle Actions

#### [Setup](docs/setup.md)
**Purpose**: Initialize the deepfreeze environment
**When**: Once, before any other operations
**What it does**: Creates first S3 bucket and repository, status index, and optional sample ILM policy

#### [Rotate](docs/rotate.md)
**Purpose**: Create new active repository and retire old ones
**When**: On schedule (weekly/monthly) or when size thresholds are met
**What it does**:
- Creates new repository (becomes the active one)
- Versions ILM policies to point to new repository
- Updates index templates to use new versioned policies
- Unmounts old repositories beyond retention limit
- Archives unmounted repositories to Glacier

#### [Status](docs/status.md)
**Purpose**: Monitor system state and health
**When**: Anytime (read-only)
**What it does**: Displays repositories, thawed state, buckets, ILM policies, and configuration

### Data Access Actions

#### [Thaw](docs/thaw.md)
**Purpose**: Restore frozen repositories from Glacier
**When**: On-demand when historical data access is needed
**What it does**:
- Identifies repositories containing snapshots in the requested date range
- Initiates AWS Glacier restore process
- Mounts repositories in Elasticsearch when restore completes
- Optionally mounts indices from snapshots for querying

**Modes**:
- **Create**: Start new thaw request for a date range
- **Check Status**: Monitor restore progress and mount when complete
- **List**: Display all active thaw requests

#### [Refreeze](docs/refreeze.md)
**Purpose**: Manually unmount thawed repositories before automatic expiration
**When**: When finished accessing historical data
**What it does**:
- Deletes mounted indices from thawed snapshots
- Unmounts repositories
- Updates metadata to frozen state
- Note: AWS restored objects remain until expiration time

### Maintenance Actions

#### [Cleanup](docs/cleanup.md)
**Purpose**: Automatic maintenance of expired thawed repositories
**When**: After every rotation (automatic) or on schedule (recommended daily)
**What it does**:
- Detects expired repositories via timestamp and S3 restore status checks
- Unmounts expired repositories
- Deletes indices that only exist in expired repositories
- Cleans up completed/failed thaw requests
- Removes orphaned ILM policies

#### [Repair Metadata](docs/repair_metadata.md)
**Purpose**: Diagnostic tool to fix metadata discrepancies
**When**: After system issues, manual S3 changes, or suspected metadata desync
**What it does**:
- Scans actual S3 storage classes for all repositories
- Compares with metadata in deepfreeze-status index
- Corrects discrepancies automatically
- Verifies and updates thaw request states

## Common Workflows

### Typical Monthly Workflow

```bash
# Day 1 of month - Rotate to new repository
curator_cli deepfreeze rotate --keep 6

# Rotation automatically:
# - Creates new active repository
# - Versions ILM policies to point to new repo
# - Updates index templates
# - Unmounts repositories older than --keep limit
# - Archives unmounted repositories to Glacier
# - Runs cleanup for expired thaws

# Monitor throughout month
curator_cli deepfreeze status
```

### Data Recovery Workflow

```bash
# 1. User needs to analyze data from Q1 2024
curator_cli deepfreeze thaw \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --duration 7 \
  --tier Standard

# Returns: Thaw request ID: abc-123-def

# 2. Wait for Glacier restore to complete (check periodically)
curator_cli deepfreeze thaw --check-status abc-123-def

# 3. Once complete, snapshots are accessible
#    Mount indices from snapshots as needed for querying

# 4. When analysis is finished (before 7 days)
curator_cli deepfreeze refreeze --thaw-request-id abc-123-def

# OR let automatic cleanup handle it after 7 days
```

### Troubleshooting Workflow

```bash
# 1. Check overall status
curator_cli deepfreeze status

# 2. Check if metadata is synchronized with S3
curator_cli deepfreeze repair-metadata --dry-run

# 3. Apply fixes if needed
curator_cli deepfreeze repair-metadata

# 4. Verify resolution
curator_cli deepfreeze status --show-repos
```

## Configuration

### Initial Setup Options

Key decisions during setup that affect ongoing operations:

**Repository Organization** (`--rotate-by`):
- `bucket`: New S3 bucket per rotation (cleaner separation, more buckets to manage)
- `path`: Same bucket, different paths per rotation (fewer buckets, shared lifecycle rules)

**Naming Strategy** (`--style`):
- `oneup`: Sequential numbering (000001, 000002, ...)
- `date`: Monthly date stamps (2025.01, 2025.02, ...)

**Storage Class** (`--storage-class`):
- `intelligent_tiering`: Automatic cost optimization (recommended)
- `standard`: Standard S3 storage
- `standard_ia`: Infrequent Access
- `onezone_ia`: Single AZ Infrequent Access
- `glacier_ir`: Glacier Instant Retrieval

### Retention Policy

The `--keep` parameter in rotate controls how many repositories remain mounted and active:
- Default: 6 repositories
- Repositories beyond this limit are unmounted and pushed to Glacier
- Consider: Data access patterns, compliance requirements, cost constraints

## Prerequisites

### Required Infrastructure

1. **Elasticsearch 8.x**
   - Healthy cluster with snapshot capability
   - For ES 8.x+: S3 support built-in

2. **AWS Account**
   - Valid credentials with S3 and Glacier permissions
   - IAM policy allowing bucket creation, object operations, restore operations

3. **Curator Installation**
   - Curator 8.x with deepfreeze module
   - Python 3.12.7+

### Required Permissions

**AWS IAM Permissions**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:PutLifecycleConfiguration",
        "s3:RestoreObject",
        "s3:GetObjectAttributes"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-prefix-*",
        "arn:aws:s3:::your-bucket-prefix-*/*"
      ]
    }
  ]
}
```

**Elasticsearch Permissions**:
- Snapshot management (create, delete repositories)
- ILM policy management (create, update, delete policies)
- Index template management (update templates)
- Index management (create, delete searchable snapshots)
- Read/write to `deepfreeze-status` index

## Cost Optimization

Costs are estimated as of this writing; please confirm them in your own account.

### Storage Costs

- **S3 Standard**: ~$0.023/GB/month (active repositories with searchable snapshots)
- **S3 Intelligent-Tiering**: Automatic optimization based on access patterns
- **S3 Glacier Instant Retrieval**: ~$0.004/GB/month (frozen repositories)
- **S3 Glacier Flexible Retrieval**: ~$0.0036/GB/month (deeper archive)

### Restore Costs (when thawing)

- **Standard Retrieval**: $0.03/GB + $0.01/1000 requests (3-5 hours)
- **Expedited Retrieval**: $0.10/GB + $0.03/1000 requests (1-5 minutes)
- **Bulk Retrieval**: $0.0025/GB + $0.025/1000 requests (5-12 hours)

### Best Practices

1. **Choose appropriate retention** (`--keep`): Balance between access needs and storage costs
2. **Use Bulk retrieval tier** for non-urgent historical analysis
3. **Minimize thaw duration**: Only request access for the time period needed
4. **Refreeze proactively**: Don't wait for automatic expiration if analysis is complete
5. **Regular rotation**: Prevents any single repository from growing too large
6. **Monitor with status**: Regular checks prevent unexpected costs

## Scheduling Recommendations

### Automated Schedule (cron)

```bash
# Rotate monthly (1st of month at 1 AM)
0 1 1 * * curator_cli deepfreeze rotate --keep 6

# Cleanup daily (3 AM)
0 3 * * * curator_cli deepfreeze cleanup

# Status report weekly (Mondays at 9 AM)
0 9 * * 1 curator_cli deepfreeze status --show-repos
```

### On-Demand Operations

- **Thaw**: User-initiated when historical data access is required
- **Refreeze**: User-initiated when analysis is complete
- **Repair Metadata**: As needed for troubleshooting or after manual interventions

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Repository Count**: Ensure rotation is happening on schedule
2. **Active Repository**: Should always be exactly one
3. **Thawed Repository Count**: Detect stuck or forgotten thaws
4. **Expired Repository Count**: Should be 0 after cleanup runs
5. **Failed Thaw Requests**: Indicate AWS or configuration issues
6. **Status Index Size**: Should grow slowly and predictably

### Health Checks

```bash
# Check for expired repositories (should be none)
curator_cli deepfreeze status --show-thawed | grep expired

# Verify metadata consistency with S3
curator_cli deepfreeze repair-metadata --dry-run

# List active thaw requests
curator_cli deepfreeze thaw --list

# Check ILM policy versioning
curator_cli deepfreeze status --show-ilm
```

## Troubleshooting

### Common Issues

**Problem**: Rotation fails with "No ILM policies found"
**Solution**: Create at least one ILM policy with searchable_snapshot action before rotating

**Problem**: Thaw request stuck in "thawing" state
**Solution**: Check AWS Glacier restore status in S3 console, run `repair-metadata` to sync state

**Problem**: Status shows incorrect thaw_state for repositories
**Solution**: Run `repair-metadata` to scan S3 storage classes and correct metadata

**Problem**: Cleanup not removing expired repositories
**Solution**: Ensure cleanup runs regularly via cron, verify Elasticsearch permissions

**Problem**: High AWS costs after thaw operation
**Solution**: Use `refreeze` to unmount when finished, reduce `--duration` on future thaw requests

### Debug Mode

All actions support `--dry-run` mode for safe testing:

```bash
# Test rotation without making changes
curator_cli --dry-run deepfreeze rotate --keep 6

# Test cleanup
curator_cli --dry-run deepfreeze cleanup

# Test metadata repair
curator_cli deepfreeze repair-metadata --dry-run
```

## Integration with ILM

Deepfreeze is designed to work alongside Elasticsearch ILM policies. Here's how they work together:

### ILM Policy Example

```json
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
            "snapshot_repository": "deepfreeze-000001"
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
}
```

The `delete_searchable_snapshot` setting is critical, and enables retention of the snapshot after index deletion.

### What Happens

1. **ILM** creates searchable snapshots in the current deepfreeze repository
2. **ILM** deletes indices after 365 days
3. **Deepfreeze** preserves the snapshots after index deletion
4. **Deepfreeze rotate** creates new repository monthly, versions ILM policies
5. **Deepfreeze rotate** archives old repositories to Glacier after all indices are deleted
6. **Deepfreeze thaw** restores repositories from Glacier when historical data is needed

## Detailed Documentation

Each action has comprehensive documentation covering prerequisites, effects, options, examples, and troubleshooting:

- **[Setup Documentation](docs/setup.md)** - Initial configuration and first repository creation
- **[Rotate Documentation](docs/rotate.md)** - Repository rotation and lifecycle management
- **[Status Documentation](docs/status.md)** - Monitoring and visibility into system state
- **[Thaw Documentation](docs/thaw.md)** - Glacier restore and data access
- **[Refreeze Documentation](docs/refreeze.md)** - Manual repository unmounting
- **[Cleanup Documentation](docs/cleanup.md)** - Automatic maintenance and expiration handling
- **[Repair Metadata Documentation](docs/repair_metadata.md)** - Metadata consistency and troubleshooting

## Support and Development

### Author

Deepfreeze was written by Bret Wortman (bret.wortman@elastic.co) and is built on the foundation of Curator, which is the work of Aaron Mildenstein and many others.

### Contributing

This is part of the Elasticsearch Curator project. For bugs, feature requests, or contributions, see the main Curator repository.

### Version

Current version: 8.0.21 (work in progress)

Part of Elasticsearch Curator 8.x
