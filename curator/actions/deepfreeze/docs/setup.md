# Setup Action

## Purpose

The Setup action initializes the deepfreeze environment by creating the first repository and S3 bucket for long-term cold storage of Elasticsearch snapshots. This is a one-time initialization step that must be performed before any other deepfreeze operations.

Setup creates:
- An S3 bucket for storing snapshots
- An Elasticsearch snapshot repository pointing to that bucket
- A status index (`deepfreeze-status`) to track repository and thaw request metadata
- (Optional) A sample ILM policy demonstrating integration with searchable snapshots

## Prerequisites

### Required Before Running Setup

1. **Elasticsearch Cluster**
   - Running Elasticsearch 7.x or 8.x
   - Cluster must be healthy and accessible
   - For ES 7.x: `repository-s3` plugin must be installed on all nodes
   - For ES 8.x+: S3 repository support is built-in

2. **AWS Credentials**
   - Valid AWS credentials with S3 permissions
   - Credentials configured in one of:
     - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
     - Elasticsearch keystore (for cluster-wide credentials)
     - AWS credentials file (`~/.aws/credentials`)

3. **IAM Permissions**
   - `s3:CreateBucket` - Create new S3 buckets
   - `s3:PutObject` - Write snapshot data
   - `s3:GetObject` - Read snapshot data
   - `s3:DeleteObject` - Clean up old snapshots
   - `s3:ListBucket` - List bucket contents
   - `s3:PutBucketAcl` - Set bucket ACL (if using canned ACLs)

4. **Clean Environment**
   - No existing repositories with the configured prefix
   - No existing `deepfreeze-status` index
   - No existing S3 buckets with the configured name

### Precondition Checks

Setup performs comprehensive validation before making any changes:

- **Status Index**: Verifies `deepfreeze-status` index does not already exist
- **Repository Prefix**: Checks no repositories match the configured prefix
- **S3 Bucket**: Confirms bucket name is available (not already in use)
- **S3 Plugin** (ES 7.x only): Validates `repository-s3` plugin is installed
- **Cluster Health**: Ensures cluster is accessible and responsive

If any precondition fails, Setup displays detailed error messages with solutions and exits without making changes.

## Effects

### What Setup Creates

1. **S3 Bucket**
   - Name: `{bucket_name_prefix}-{suffix}` (if `rotate_by=bucket`)
   - OR: `{bucket_name_prefix}` (if `rotate_by=path`)
   - Region: Determined by AWS credentials/configuration
   - Storage class: As configured (default: `intelligent_tiering`)
   - ACL: As configured (default: `private`)

2. **Elasticsearch Snapshot Repository**
   - Name: `{repo_name_prefix}-{suffix}`
   - Type: `s3`
   - Settings:
     - `bucket`: The created S3 bucket name
     - `base_path`: `{base_path_prefix}-{suffix}` (or just `{base_path_prefix}` if `rotate_by=bucket`)
     - `canned_acl`: As configured
     - `storage_class`: As configured

3. **Status Index** (`deepfreeze-status`)
   - Stores configuration settings
   - Tracks repository metadata (mount status, thaw state, date ranges)
   - Records thaw request history
   - Schema is created automatically

4. **Configuration Document**
   - Saved in `deepfreeze-status` index
   - Contains all deepfreeze settings:
     - Repository and bucket naming patterns
     - Rotation strategy (`bucket` or `path`)
     - Naming style (`oneup` or date-based)
     - Storage class and ACL settings
     - Last-used suffix for rotation

5. **Sample ILM Policy** (Optional)
   - Name: `{ilm_policy_name}` (default: `deepfreeze-sample-policy`)
   - Demonstrates integration with searchable snapshots
   - Phases:
     - **Hot**: Rollover at 45GB or 7 days
     - **Frozen**: Convert to searchable snapshot after 14 days
     - **Delete**: Delete index after 365 days (preserves snapshot)

### State Changes

- Elasticsearch cluster gains a new snapshot repository
- AWS S3 account gains a new bucket
- Deepfreeze system transitions from uninitialized to operational

### What Setup Does NOT Do

- Does not create any snapshots
- Does not modify existing repositories or indices
- Does not configure ILM policies on indices (except optional sample)
- Does not modify Elasticsearch cluster settings

## Options

### Required Configuration

These settings must be provided (either have sensible defaults or are required):

#### `repo_name_prefix`
- **Type**: String
- **Default**: `deepfreeze`
- **Description**: Prefix for repository names. Repositories are named `{prefix}-{suffix}`
- **Example**: `repo_name_prefix="myapp"` creates repositories like `myapp-000001`, `myapp-000002`

#### `bucket_name_prefix`
- **Type**: String
- **Default**: `deepfreeze`
- **Description**: Prefix for S3 bucket names (or full bucket name if `rotate_by=path`)
- **Example**: `bucket_name_prefix="mycompany-es-cold"` creates buckets like `mycompany-es-cold-000001`
- **Important**: Bucket names must be globally unique across all AWS accounts

#### `base_path_prefix`
- **Type**: String
- **Default**: `snapshots`
- **Description**: Path within the S3 bucket where snapshots are stored
- **Example**: `base_path_prefix="elasticsearch/backups"` stores snapshots under `s3://bucket/elasticsearch/backups-000001/`

### Storage Configuration

#### `storage_class`
- **Type**: String
- **Default**: `intelligent_tiering`
- **Options**:
  - `standard` - S3 Standard (frequent access)
  - `standard_ia` - Infrequent Access
  - `intelligent_tiering` - Automatic tiering
  - `glacier_instant_retrieval` - Instant retrieval from Glacier
  - `glacier_flexible_retrieval` - Minutes-hours retrieval (not recommended for searchable snapshots)
  - `glacier_deep_archive` - Hours retrieval (not recommended for searchable snapshots)
- **Description**: AWS S3 storage class for snapshot objects
- **Recommendation**: Use `intelligent_tiering` for automatic cost optimization, or `glacier_instant_retrieval` for long-term cold storage with instant access capability

#### `canned_acl`
- **Type**: String
- **Default**: `private`
- **Options**: `private`, `public-read`, `public-read-write`, `authenticated-read`, `bucket-owner-read`, `bucket-owner-full-control`
- **Description**: AWS S3 canned ACL applied to the bucket
- **Security**: Use `private` unless you have specific requirements
- **Reference**: [AWS S3 Canned ACL Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)

### Rotation Strategy

#### `rotate_by`
- **Type**: String
- **Default**: `path`
- **Options**: `bucket`, `path`
- **Description**: Determines how repositories are isolated when rotating
  - `bucket`: Each rotation creates a new S3 bucket
  - `path`: All rotations use the same bucket with different base paths
- **Use Cases**:
  - `bucket`: Better for compliance/auditing (each period is completely isolated)
  - `path`: More cost-effective (single bucket, easier management)
- **Example**:
  - `rotate_by=bucket`: `s3://myapp-000001/snapshots`, `s3://myapp-000002/snapshots`
  - `rotate_by=path`: `s3://myapp/snapshots-000001`, `s3://myapp/snapshots-000002`

#### `style`
- **Type**: String
- **Default**: `oneup`
- **Options**: `oneup`, `date`
- **Description**: Naming convention for repository suffixes
  - `oneup`: Sequential numbering (000001, 000002, ...)
  - `date`: Date-based (YYYY.MM format)
- **Use Cases**:
  - `oneup`: Simple, no dependency on current date
  - `date`: Clear temporal organization (requires `--year` and `--month` flags)

#### `year` and `month`
- **Type**: Integer
- **Default**: None (not used with `style=oneup`)
- **Description**: Override year/month for date-based suffixes
- **Example**: `--year 2025 --month 1` creates suffix `2025.01`
- **Required**: Only when `style=date`

### Cloud Provider

#### `provider`
- **Type**: String
- **Default**: `aws`
- **Options**: Currently only `aws` is supported
- **Description**: Cloud provider for object storage
- **Future**: May support Azure, GCP in future releases

### Optional Features

#### `create_sample_ilm_policy`
- **Type**: Boolean
- **Default**: `False`
- **Description**: Create a sample ILM policy demonstrating deepfreeze integration
- **Use Case**: Educational/demonstration purposes, or as a starting template
- **Warning**: This is an example policy; review and customize for production use

#### `ilm_policy_name`
- **Type**: String
- **Default**: `deepfreeze-sample-policy`
- **Description**: Name for the sample ILM policy
- **Only Used**: When `create_sample_ilm_policy=True`

### Output Format

#### `porcelain`
- **Type**: Boolean
- **Default**: `False`
- **Description**: Output machine-readable tab-separated values instead of rich formatted text
- **Use Case**: Scripting, automation, CI/CD pipelines
- **Output Format**:
  - Success: `SUCCESS\t{repo_name}\t{bucket_name}\t{base_path}`
  - Error: `ERROR\t{error_type}\t{error_message}`

## Usage Examples

### Basic Setup (Defaults)

```bash
curator_cli deepfreeze setup
```

Creates:
- Repository: `deepfreeze-000001`
- Bucket: `deepfreeze` (with path rotation)
- Base path: `snapshots-000001`

### Custom Naming

```bash
curator_cli deepfreeze setup \
  --repo-name-prefix myapp \
  --bucket-name-prefix mycompany-es-cold \
  --base-path-prefix backups
```

Creates:
- Repository: `myapp-000001`
- Bucket: `mycompany-es-cold` (with path rotation)
- Base path: `backups-000001`

### Bucket-Based Rotation

```bash
curator_cli deepfreeze setup \
  --rotate-by bucket
```

Creates:
- Repository: `deepfreeze-000001`
- Bucket: `deepfreeze-000001` (new bucket per rotation)
- Base path: `snapshots` (static)

### Date-Based Rotation

```bash
curator_cli deepfreeze setup \
  --style date \
  --year 2025 \
  --month 1
```

Creates:
- Repository: `deepfreeze-2025.01`
- Bucket: `deepfreeze`
- Base path: `snapshots-2025.01`

### With Sample ILM Policy

```bash
curator_cli deepfreeze setup \
  --create-sample-ilm-policy \
  --ilm-policy-name my-tiering-policy
```

Creates everything plus ILM policy `my-tiering-policy`

### Custom Storage Class

```bash
curator_cli deepfreeze setup \
  --storage-class glacier_instant_retrieval
```

Optimizes for long-term cold storage with instant retrieval capability

### Scripting/Automation

```bash
curator_cli deepfreeze setup --porcelain > setup_result.txt
if grep -q "^SUCCESS" setup_result.txt; then
  echo "Setup completed successfully"
else
  echo "Setup failed:"
  cat setup_result.txt
  exit 1
fi
```

## Error Handling

### Common Errors and Solutions

#### 1. Status Index Already Exists

**Error**: `Status index deepfreeze-status already exists`

**Cause**: Deepfreeze has already been initialized

**Solutions**:
- If this is intentional, delete the existing setup:
  ```bash
  curator_cli --host localhost DELETE index --name deepfreeze-status
  ```
- If you want to keep the existing setup, use `rotate` instead of `setup`

#### 2. Repository Prefix Exists

**Error**: `Found N existing repositories matching prefix deepfreeze`

**Cause**: Repositories with the configured prefix already exist

**Solutions**:
- Choose a different `repo_name_prefix`
- Delete existing repositories (⚠️ WARNING: Ensure you have backups!)
  ```bash
  curator_cli deepfreeze cleanup
  ```

#### 3. S3 Bucket Already Exists

**Error**: `S3 bucket deepfreeze-000001 already exists`

**Cause**: Bucket name is already in use (either by you or globally by another AWS account)

**Solutions**:
- Choose a different `bucket_name_prefix`
- Delete the existing bucket (⚠️ WARNING: This deletes all data!)
  ```bash
  aws s3 rb s3://deepfreeze-000001 --force
  ```
- If the bucket is owned by another account, you must use a different name

#### 4. S3 Repository Plugin Not Installed (ES 7.x)

**Error**: `Elasticsearch S3 repository plugin is not installed`

**Cause**: ES 7.x requires the `repository-s3` plugin

**Solution**:
```bash
# On each Elasticsearch node:
bin/elasticsearch-plugin install repository-s3
# Then restart all nodes
```

#### 5. AWS Credentials Not Found

**Error**: `Failed to create bucket: The security token included in the request is invalid`

**Cause**: AWS credentials are missing or invalid

**Solutions**:
- Set environment variables:
  ```bash
  export AWS_ACCESS_KEY_ID=your_access_key
  export AWS_SECRET_ACCESS_KEY=your_secret_key
  ```
- Configure Elasticsearch keystore:
  ```bash
  bin/elasticsearch-keystore add s3.client.default.access_key
  bin/elasticsearch-keystore add s3.client.default.secret_key
  ```

#### 6. Insufficient IAM Permissions

**Error**: `Failed to create repository: Access Denied`

**Cause**: AWS credentials lack required S3 permissions

**Solution**: Ensure IAM policy includes:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::deepfreeze*",
        "arn:aws:s3:::deepfreeze*/*"
      ]
    }
  ]
}
```

## Best Practices

### Before Setup

1. **Plan Your Naming Convention**: Choose prefixes that are:
   - Descriptive (e.g., `myapp-prod-es-cold`)
   - Compliant with AWS naming rules (lowercase, hyphens only)
   - Globally unique for bucket names

2. **Choose Storage Class Carefully**:
   - Use `intelligent_tiering` for automatic optimization
   - Use `glacier_instant_retrieval` for long-term cold storage
   - Avoid `glacier_flexible_retrieval` or `glacier_deep_archive` for searchable snapshots

3. **Decide Rotation Strategy**:
   - `rotate_by=bucket`: Better isolation, compliance, auditing
   - `rotate_by=path`: More cost-effective, simpler management

4. **Test AWS Credentials**:
   ```bash
   aws s3 ls  # Verify credentials work
   ```

### After Setup

1. **Verify Repository**:
   ```bash
   curator_cli deepfreeze status --show-config --show-repos
   ```

2. **Configure ILM Policies**: Update your existing ILM policies to use the new repository:
   ```json
   {
     "policy": {
       "phases": {
         "frozen": {
           "actions": {
             "searchable_snapshot": {
               "snapshot_repository": "deepfreeze-000001"
             }
           }
         },
         "delete": {
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

3. **Set Up Rotation Schedule**: Plan when to run `rotate` action (typically monthly or when repository reaches size limits)

4. **Document Your Configuration**: Save your setup parameters for disaster recovery:
   ```bash
   curator_cli deepfreeze status --show-config --porcelain > deepfreeze-config.txt
   ```

## Related Actions

- **Rotate**: Create new repositories and retire old ones
- **Status**: View current configuration and repository state
- **Thaw**: Restore data from cold storage for access

## Security Considerations

- **Bucket ACLs**: Use `private` unless you have specific requirements
- **IAM Policies**: Follow principle of least privilege
- **Encryption**: Enable S3 bucket encryption at rest
- **Credentials**: Store in Elasticsearch keystore, not in plain text
- **Network**: Use VPC endpoints for S3 to avoid internet traffic

## Performance Considerations

- **Storage Class**: `intelligent_tiering` has no retrieval fees for frequent access
- **Bucket vs Path**: Path rotation is faster (no bucket creation overhead)
- **Region**: Ensure S3 bucket and ES cluster are in the same AWS region to minimize latency and transfer costs
