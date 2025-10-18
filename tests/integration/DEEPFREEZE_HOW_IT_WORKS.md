# How Deepfreeze Works: A Complete Guide

## Overview

Deepfreeze is a system for archiving Elasticsearch data to AWS S3 Glacier using Elasticsearch's native **searchable snapshots** feature integrated with **Index Lifecycle Management (ILM)**.

## Core Concept

**Deepfreeze does NOT manage snapshots directly.** Instead, it manages:
1. **Elasticsearch snapshot repositories** (S3-backed)
2. **ILM policies** that control when indices become searchable snapshots
3. **Repository rotation** to move old snapshots to Glacier Deep Archive

The actual snapshot creation and mounting is handled by **Elasticsearch ILM**.

---

## The Complete Workflow

### Phase 1: Initial Setup (`deepfreeze setup`)

**What happens:**
1. Creates an S3 bucket (e.g., `my-bucket`)
2. Creates an Elasticsearch snapshot repository pointing to that bucket (e.g., `deepfreeze-000001`)
3. Saves configuration to a status index (`.deepfreeze-status-idx`)

**Result:**
- You now have a repository that ILM policies can reference for searchable snapshots
- NO snapshots exist yet
- NO indices are frozen yet

**Key Point:** Setup is a one-time operation. It creates the **first repository**.

---

### Phase 2: ILM Manages Data (`elasticsearch` handles this)

**User creates ILM policies** that reference the deepfreeze repository:

```json
{
  "policy": {
    "phases": {
      "frozen": {
        "min_age": "30m",
        "actions": {
          "searchable_snapshot": {
            "snapshot_repository": "backups",
            "force_merge_index": true
          }
        }
      },
      "delete": {
        "min_age": "60m",
        "actions": {
          "delete": {
            "delete_searchable_snapshot": false
          }
        }
      },
      "cold": {
        "min_age": "7m",
        "actions": {
          "allocate": {
            "number_of_replicas": 0,
            "include": {},
            "exclude": {},
            "require": {}
          },
          "searchable_snapshot": {
            "snapshot_repository": "backups",
            "force_merge_index": true
          },
          "set_priority": {
            "priority": 0
          }
        }
      },
      "hot": {
        "min_age": "0ms",
        "actions": {
          "forcemerge": {
            "max_num_segments": 1
          },
          "rollover": {
            "max_age": "3m",
            "max_primary_shard_size": "40gb"
          },
          "set_priority": {
            "priority": 100
          },
          "shrink": {
            "number_of_shards": 1,
            "allow_write_after_shrink": false
          }
        }
      }
    }
  }
}
```

**What Elasticsearch does automatically:**
1. **Hot phase**: Index is writable, stored on local disk with fast SSD access
2. **Rollover**: When index hits max_age/max_size, new index is created
3. **Cold phase**: Index transitions to cold tier (still on disk, but can be on slower/cheaper storage)
   - Index remains fully searchable
   - Data is on disk but may be moved to less expensive nodes
   - The index name changes: `my-index-000001` → `restored-my-index-000001`
4. **Frozen phase**: Elasticsearch:
   - Creates a snapshot in `deepfreeze-000001` repository
   - Deletes the local index
   - Mounts the snapshot as a **searchable snapshot** (read-only, backed by S3)
   - The index name changes: `restored-my-index-000001` → `partial-restored-my-index-000001`
5. **Delete phase**: Elasticsearch:
   - Deletes the mounted searchable snapshot index
   - KEEPS the snapshot in S3 (because `delete_searchable_snapshot: false`)

**Key Point:** Deepfreeze does NOT trigger snapshots. ILM does this automatically based on index age.

---

### Phase 3: Repository Rotation (`deepfreeze rotate`)

**Rotation happens periodically** (e.g., monthly, or on-demand) to:
1. Create a **new repository** (e.g., `deepfreeze-000002`)
2. Create a new, versioned ILM policy which uses the **new repository** for future snapshots
3. Unmount old repositories and push them to Glacier Deep Archive
4. Clean up old ILM policy versions

**Step-by-step what happens:**

#### 3.1: Create New Repository
```python
# Creates: deepfreeze-000002
# With either:
#   - New S3 bucket: my-bucket-000002 (if rotate_by=bucket)
#   - New S3 path: my-bucket/snapshots-000002 (if rotate_by=path)
```

#### 3.2: Version ILM Policies

**CRITICAL**: Deepfreeze does NOT modify existing policies. It creates **versioned copies**:

```
Old policy: my-ilm-policy-000001 → references deepfreeze-000001
New policy: my-ilm-policy-000002 → references deepfreeze-000002
```

This ensures:
- Old indices keep their old policies and can still access old snapshots
- New indices use new policies with the new repository
- No disruption to existing data
- Index template updates to point to latest versioned ILM policy

#### 3.3: Update Index Templates

All index templates are updated to use the new versioned policies:

```yaml
# Before rotation:
template: logs-*
  settings:
    index.lifecycle.name: my-ilm-policy-000001

# After rotation:
template: logs-*
  settings:
    index.lifecycle.name: my-ilm-policy-000002
```

**Result**: New indices created from this template will use the new policy.

#### 3.4: Update Repository Date Ranges

For each **mounted** repository, deepfreeze scans the searchable snapshot indices to determine:
- `earliest`: Timestamp of oldest document across all mounted indices
- `latest`: Timestamp of newest document across all mounted indices

These are stored in the status index for tracking.

#### 3.5: Unmount Old Repositories

Based on the `keep` parameter (default: 6), deepfreeze:
1. Sorts repositories by version (newest first)
2. Keeps the first N repositories mounted
3. Unmounts older repositories:
   - Deletes all searchable snapshot indices from that repo (e.g., `partial-my-index-*`)
   - Deletes the Elasticsearch repository definition
   - Marks the repository as "unmounted" in the status index
   - The underlying S3 bucket/path still contains the snapshots

#### 3.6: Push to Glacier Deep Archive

For each unmounted repository:
```python
# Changes S3 storage class from Intelligent-Tiering to Glacier Deep Archive
push_to_glacier(s3_client, repository)
```

This reduces storage costs dramatically (S3 → Glacier Deep Archive = ~95% cost reduction).

#### 3.7: Cleanup Old ILM Policies

For each unmounted repository, deepfreeze:
1. Finds all ILM policies with the same version suffix (e.g., `-000001`)
2. Checks if they're still in use by any:
   - Indices
   - Data streams
   - Index templates
3. Deletes policies that are no longer in use

**Example**:
- Repository `deepfreeze-000001` is unmounted
- Policy `my-ilm-policy-000001` exists
- No indices use this policy
- No templates reference this policy
- → Policy is deleted

---

## Storage Lifecycle Summary

```
1. Hot Index (local disk - hot tier):
   - Writable
   - Fast queries (SSD)
   - Stored on ES hot tier data nodes
   - Cost: High (fast SSD storage)

2. Cold Index (local disk - cold tier):
   - Read-only
   - Good query performance
   - Stored on ES cold tier data nodes (cheaper disks)
   - Cost: Medium (standard disk storage)

3. Frozen Index (searchable snapshot, S3):
   - Read-only
   - Slower queries (S3 latency)
   - Stored in S3 (Intelligent-Tiering)
   - Repository is "mounted"
   - Cost: Low (S3)

4. Archived Snapshot (Glacier Deep Archive):
   - Not queryable
   - Repository is "unmounted"
   - Stored in Glacier Deep Archive
   - Cost: Very low (~$1/TB/month)
   - Retrieval time: 12-48 hours (if needed)
```

---

## Key Data Structures

### 1. Status Index (`.deepfreeze-status-idx`)

Stores two types of documents:

**Settings Document** (`_id: deepfreeze-settings`):
```json
{
  "repo_name_prefix": "deepfreeze",
  "bucket_name_prefix": "my-bucket",
  "base_path_prefix": "snapshots",
  "storage_class": "intelligent_tiering",
  "rotate_by": "path",
  "last_suffix": "000003",
  "provider": "aws",
  "style": "oneup"
}
```

**Repository Documents** (`_id: {repo_name}`):
```json
{
  "name": "deepfreeze-000002",
  "bucket": "my-bucket",
  "base_path": "/snapshots-000002",
  "earliest": 1704067200000,  // Unix timestamp
  "latest": 1735689600000,    // Unix timestamp
  "is_thawed": false,
  "is_mounted": true,
  "indices": [
    "partial-logs-2024.01.01-000001",
    "partial-logs-2024.01.02-000001"
  ]
}
```

### 2. Repository Naming

**Format**: `{prefix}-{suffix}`

**Two styles:**
- **oneup** (default): `deepfreeze-000001`, `deepfreeze-000002`, etc.
- **date**: `deepfreeze-2024.01`, `deepfreeze-2024.02`, etc.

### 3. ILM Policy Versioning

**Pattern**: `{base_name}-{suffix}`

Example progression:
```
Setup:      my-policy          (created by user)
Rotate 1:   my-policy-000001   (created by deepfreeze)
Rotate 2:   my-policy-000002   (created by deepfreeze)
Rotate 3:   my-policy-000003   (created by deepfreeze)
```

The original `my-policy` can be deleted after first rotation.

---

## Critical Configuration Points

### 1. ILM Delete Action

**MUST set** `delete_searchable_snapshot: false`:

```json
{
  "delete": {
    "actions": {
      "delete": {
        "delete_searchable_snapshot": false  // ← CRITICAL!
      }
    }
  }
}
```

Without this, Elasticsearch will delete snapshots when indices are deleted, defeating the entire purpose of deepfreeze.

### 2. Rotation Frequency

Rotation should happen **BEFORE** repositories get too large:

**Recommended**: Rotate every 30-90 days depending on:
- Snapshot size
- Number of searchable snapshot indices
- S3 transfer costs for Glacier transitions
- Only push to Glacier after the value of the data has decreased to the point that it's unlikely to be queried any longer.

**Why**: Once a repository is pushed to Glacier, you cannot query those snapshots without restoring them first (12-48 hour delay).

### 3. Keep Parameter

**Default**: `keep=6`

Keeps the 6 most recent repositories mounted (queryable). Older repositories are unmounted and pushed to Glacier.

**Tuning**:
- **Higher keep**: More data queryable, higher S3 costs
- **Lower keep**: Less data queryable, lower costs, more in Glacier

---

## Testing Workflow

### Manual Testing Steps:

1. **Setup** (once):
   ```bash
   curator_cli deepfreeze setup \
     --bucket-name my-test-bucket \
     --repo-name deepfreeze
   ```

2. **Create ILM Policy** (once):
   ```bash
   curl -X PUT "localhost:9200/_ilm/policy/logs-policy" \
     -H 'Content-Type: application/json' \
     -d '{
  "policy": {
    "phases": {
      "frozen": {
        "min_age": "30m",
        "actions": {
          "searchable_snapshot": {
            "snapshot_repository": "backups",
            "force_merge_index": true
          }
        }
      },
      "delete": {
        "min_age": "60m",
        "actions": {
          "delete": {
            "delete_searchable_snapshot": false
          }
        }
      },
      "cold": {
        "min_age": "7m",
        "actions": {
          "allocate": {
            "number_of_replicas": 0,
            "include": {},
            "exclude": {},
            "require": {}
          },
          "searchable_snapshot": {
            "snapshot_repository": "backups",
            "force_merge_index": true
          },
          "set_priority": {
            "priority": 0
          }
        }
      },
      "hot": {
        "min_age": "0ms",
        "actions": {
          "forcemerge": {
            "max_num_segments": 1
          },
          "rollover": {
            "max_age": "3m",
            "max_primary_shard_size": "40gb"
          },
          "set_priority": {
            "priority": 100
          },
          "shrink": {
            "number_of_shards": 1,
            "allow_write_after_shrink": false
          }
        }
      }
    }
  }
}'
   ```

3. **Create Index Template** (once):
   ```bash
   curl -X PUT "localhost:9200/_index_template/logs-template" \
     -H 'Content-Type: application/json' \
     -d '{
       "index_patterns": ["logs-*"],
       "template": {
         "settings": {
           "index.lifecycle.name": "logs-policy",
           "index.lifecycle.rollover_alias": "logs"
         }
       }
     }'
   ```

4. **Create Initial Index** (once):
   ```bash
   curl -X PUT "localhost:9200/logs-2024.01.01-000001" \
     -H 'Content-Type: application/json' \
     -d '{
       "aliases": {
         "logs": {"is_write_index": true}
       }
     }'
   ```

5. **Index Data** (ongoing):
   ```bash
   curl -X POST "localhost:9200/logs/_doc" \
     -H 'Content-Type: application/json' \
     -d '{"message": "test log", "timestamp": "2024-01-01T00:00:00Z"}'
   ```

6. **Wait for ILM** (automatic):
   - After 1 day: Index rolls over
   - After 7 days from creation: Index moves to cold phase
   - After 30 days from creation: Index becomes frozen (searchable snapshot)
   - After 365 days from creation: Index is deleted (snapshot remains)

7. **Rotate** (periodic):
   ```bash
   curator_cli deepfreeze rotate --keep 6
   ```

---

## Common Misconceptions

### ❌ "Deepfreeze creates snapshots"
**NO.** Elasticsearch ILM creates snapshots when indices reach the frozen phase.

### ❌ "Rotate command snapshots data"
**NO.** Rotate creates a new repository, updates policies, and unmounts old repos. ILM handles snapshots.

### ❌ "I need to run rotate after every snapshot"
**NO.** Rotate is periodic (monthly/quarterly). ILM creates snapshots automatically whenever indices age into frozen phase.

### ❌ "Unmounted repos are deleted"
**NO.** Unmounted repos have their snapshots preserved in S3, just moved to Glacier Deep Archive for cheaper storage.

### ❌ "Old ILM policies are modified"
**NO.** Old policies are left unchanged. New versioned policies are created.

---

## Integration Test Requirements

Given the above, integration tests should verify:

1. **Setup**:
   - Creates repository
   - Creates status index
   - Saves settings

2. **ILM Integration** (NOT deepfreeze responsibility):
   - Indices transition to frozen phase
   - Snapshots are created
   - Searchable snapshots are mounted

3. **Rotate**:
   - Creates new repository
   - Creates versioned ILM policies
   - Updates templates
   - Updates repository date ranges
   - Unmounts old repositories
   - Pushes to Glacier
   - Cleans up old policies

4. **Status**:
   - Reports current repositories
   - Shows mounted vs unmounted
   - Shows date ranges

5. **Cleanup**:
   - Removes thawed repositories after expiration

---

## Timing Considerations for Tests

**Real-world timing:**
- Rollover: 7 days
- Move to Cold: 7 days after creation
- Move to Frozen: 30 days after creation
- Delete: 365 days after creation
- Rotate: Monthly (30 days)

**Test timing options:**
1. **Mock ILM**: Don't wait for real ILM, manually create searchable snapshots
2. **Fast ILM**: Set phases to seconds (hot=7s, cold=7s, frozen=30s, delete=45s)
3. **Hybrid**: Use fast ILM for lifecycle tests, mocks for rotate tests

**Recommended for testing:**
- Use environment variable to control interval scaling
- All timing expressed as multiples of a base interval
- Default interval=1s for CI/CD, interval=60s for validation

