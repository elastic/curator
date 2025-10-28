# Deepfreeze Integration Tests

Comprehensive integration tests for Elasticsearch Curator's Deepfreeze functionality, covering thaw, refreeze, and cleanup operations against real AWS S3/Glacier storage.

## Test Status

### ✅ SAFE Tests (Work with Existing Data Only)

- **test_operations_on_already_thawed_data**: Read-only operations on existing thaw requests
- **test_new_thaw_request_full_lifecycle**: Creates thaw requests for existing repos (NO new indices)

### ⚠️ Work In Progress (Still Create Test Data)

The following tests are NOT yet updated and still create test indices:
- **test_thaw_complete_then_refreeze**: Creates test indices
- **test_multiple_concurrent_thaw_requests**: Creates test indices
- **test_one_day_duration_with_cleanup**: Creates test indices
- **test_cleanup_mixed_expiration_states**: Creates test indices

**Do NOT run these WIP tests against production!**

### Requirements

All tests expect:
- Existing deepfreeze setup (`deepfreeze setup` already run)
- Existing frozen repositories in the status index
- Valid AWS credentials configured
- Sufficient time for Glacier operations (up to 6-30 hours)

## Overview

These tests validate the complete lifecycle of deepfreeze operations:
- Creating and monitoring thaw requests
- Glacier restore operations (up to 6 hours)
- Repository mounting and index accessibility
- Refreeze operations
- Automated cleanup of expired thaw requests
- Data integrity verification

## Test Files

### `test_deepfreeze_integration.py`

Comprehensive integration tests that run against real infrastructure:

| Test | Duration | Description |
|------|----------|-------------|
| `test_operations_on_already_thawed_data` | ~15-30 min | Operations on existing thawed data |
| `test_new_thaw_request_full_lifecycle` | Up to 6 hours | Complete thaw lifecycle |
| `test_one_day_duration_with_cleanup` | ~30 hours | 1-day duration + 24hr wait + cleanup |
| `test_thaw_complete_then_refreeze` | Up to 6.5 hours | Thaw + immediate refreeze |
| `test_multiple_concurrent_thaw_requests` | Up to 6.5 hours | Multiple simultaneous thaws |
| `test_cleanup_mixed_expiration_states` | ~30 min | Cleanup with mixed states |

### `test_deepfreeze_thaw.py`

Existing thaw functionality tests with fast mode support.

### `test_deepfreeze_setup.py`

Tests for initial deepfreeze setup and configuration.

### `test_deepfreeze_rotate.py`

Tests for repository rotation functionality.

## Requirements

### AWS Infrastructure
- Valid AWS credentials configured (`~/.aws/credentials` or environment variables)
- S3 bucket permissions (create, delete, list)
- Sufficient AWS Glacier quota for restore operations
- Recommended: Separate test account or isolated S3 bucket namespace

### Elasticsearch Cluster
- Accessible Elasticsearch cluster (configured in `curator.yml`)
- Snapshot repository path configured (`path.repo` in `elasticsearch.yml`)
- Sufficient storage for test indices and snapshots

### Time
- Standard tests: 15-30 minutes
- Long-running tests: 6-30+ hours
- Full suite: Up to 30+ hours total

## Configuration

### Environment Variables

```bash
# Curator configuration file (default: ~/.curator/curator.yml)
export CURATOR_CONFIG=/path/to/curator.yml

# Skip long-running tests (>1 hour) - useful for CI
export DEEPFREEZE_SKIP_LONG_TESTS=1

# Fast mode: Use simulated operations for development
export DEEPFREEZE_FAST_MODE=1

# Elasticsearch server (for unit tests)
export TEST_ES_SERVER=http://localhost:9200
```

### curator.yml Example

```yaml
elasticsearch:
  client:
    hosts: http://localhost:9200
    request_timeout: 300
  other_settings:
    skip_version_test: True

logging:
  loglevel: INFO
  logfile: /var/log/curator/curator.log
  logformat: default
```

## Running Tests

### Quick Development Testing (Fast Mode)

Use fast mode for rapid development iteration with simulated Glacier operations:

```bash
# Run all tests in fast mode
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_integration.py -v

# Run specific test
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_operations_on_already_thawed_data -v -s
```

### Standard Integration Testing

Run short tests that complete in under 1 hour:

```bash
# Skip long-running tests
DEEPFREEZE_SKIP_LONG_TESTS=1 pytest tests/integration/test_deepfreeze_integration.py -v
```

### Full Integration Testing

**WARNING: These tests take 6-30+ hours and use real AWS Glacier**

```bash
# Run all tests including long-running ones
pytest tests/integration/test_deepfreeze_integration.py -v

# Run only the 24-hour cleanup test
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_one_day_duration_with_cleanup -v -s

# Run only 6-hour thaw tests
pytest tests/integration/test_deepfreeze_integration.py -v -m "not slow"

# Run only the 30-hour test
pytest tests/integration/test_deepfreeze_integration.py -v -m slow
```

### Individual Test Execution

```bash
# Operations on already-thawed data (~15-30 min)
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_operations_on_already_thawed_data -v -s

# Full lifecycle test (~6 hours)
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_new_thaw_request_full_lifecycle -v -s

# Thaw + refreeze test (~6.5 hours)
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_thaw_complete_then_refreeze -v -s

# Multiple concurrent requests (~6.5 hours)
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_multiple_concurrent_thaw_requests -v -s

# Cleanup with mixed states (~30 min)
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_cleanup_mixed_expiration_states -v -s

# 24-hour cleanup test (~30 hours) - REQUIRES REAL TIME
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_one_day_duration_with_cleanup -v -s
```

## Test Execution Tips

### 1. Monitor Long-Running Tests

Long tests log progress regularly. Use `-s` flag to see output in real-time:

```bash
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_thaw_complete_then_refreeze -v -s
```

Example output:
```
================================================================================
TEST: Thaw Complete Then Refreeze
================================================================================
Setting up test environment...
Environment ready: bucket=deepfreeze-integration-abc123xyz, repo=df-test-repo-000001
Creating index test-logs-20240101-000 with 100 docs from 2024-01-01 to 2024-01-31
...
--- Waiting for thaw to complete (up to 6.0 hours) ---
Check #1 at 0.0 minutes elapsed
Progress: 0/1 repositories mounted
Repo df-test-repo-000001: 125/543 objects restored
Sleeping for 15 minutes...
Check #2 at 15.0 minutes elapsed
...
```

### 2. Background Execution

For very long tests, run in background with output redirection:

```bash
# Run test in background
nohup pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_one_day_duration_with_cleanup -v -s > deepfreeze_test.log 2>&1 &

# Monitor progress
tail -f deepfreeze_test.log
```

### 3. CI/CD Integration

Example GitHub Actions configuration:

```yaml
name: Deepfreeze Integration Tests

on:
  # Run on PR for fast tests only
  pull_request:
    paths:
      - 'curator/actions/deepfreeze/**'

  # Full test suite on schedule (weekly)
  schedule:
    - cron: '0 2 * * 0'  # Sunday 2am

jobs:
  fast-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -e .[test]

      - name: Run fast tests
        env:
          DEEPFREEZE_FAST_MODE: 1
        run: pytest tests/integration/test_deepfreeze_integration.py -v

  long-tests:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    timeout-minutes: 450  # 7.5 hours
    steps:
      - uses: actions/checkout@v3

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1

      - name: Set up Elasticsearch
        uses: elastic/elastic-github-actions/elasticsearch@master
        with:
          stack-version: 8.11.0

      - name: Run 6-hour tests
        run: pytest tests/integration/test_deepfreeze_integration.py -v -m "not slow"
```

## Test Coverage

### Data Validation Tests

Each test includes comprehensive validation:

- **Index Searchability**: Verify indices are searchable and return expected results
- **Document Count Verification**: Ensure document counts match before/after operations
- **Timestamp Range Validation**: Verify @timestamp values fall within expected ranges
- **Repository State Verification**: Check mounted status and thaw_state
- **Request Status Tracking**: Monitor thaw request lifecycle states

### Error Scenarios

Tests handle and verify various error conditions:

- Timeout scenarios for long-running operations
- Repository conflicts with concurrent operations
- Cleanup behavior with expired vs active data
- Refreeze behavior with already-frozen repositories
- Status checks on non-existent or completed requests

## Troubleshooting

### Common Issues

#### 1. AWS Credentials Not Found

**Error**: `NoCredentialsError: Unable to locate credentials`

**Solution**: Configure AWS credentials:
```bash
# Option 1: Configure AWS CLI
aws configure

# Option 2: Set environment variables
export AWS_ACCESS_KEY_ID=your_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

#### 2. Elasticsearch Connection Failed

**Error**: `ConnectionError: Unable to connect to Elasticsearch`

**Solution**: Verify Elasticsearch is running and accessible:
```bash
# Check Elasticsearch status
curl http://localhost:9200

# Verify curator.yml configuration
cat ~/.curator/curator.yml

# Test with custom config
CURATOR_CONFIG=/path/to/custom.yml pytest tests/integration/test_deepfreeze_integration.py -v
```

#### 3. S3 Bucket Permissions

**Error**: `AccessDenied: Access Denied`

**Solution**: Ensure AWS user has required S3 permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:ListBucket",
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:RestoreObject",
        "s3:GetObjectAttributes"
      ],
      "Resource": [
        "arn:aws:s3:::deepfreeze-*",
        "arn:aws:s3:::deepfreeze-*/*"
      ]
    }
  ]
}
```

#### 4. Test Timeout

**Error**: `AssertionError: Thaw did not complete within 6 hours`

**Cause**: AWS Glacier restore taking longer than expected

**Solution**:
- Check AWS Glacier retrieval tier (Standard = 3-5 hours, Expedited = 1-5 minutes)
- Verify S3 bucket is in expected region
- Check AWS Service Health Dashboard
- Consider using FAST_MODE for development

#### 5. Repository Already Exists

**Error**: `RepositoryException: [df-test-repo-000001] repository already exists`

**Solution**: Clean up from previous failed test:
```bash
# Delete test repositories
curl -X DELETE "localhost:9200/_snapshot/df-test-repo-*"

# Delete status index
curl -X DELETE "localhost:9200/.deepfreeze-status"

# Clean up S3 buckets
aws s3 rb s3://deepfreeze-test-* --force
```

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
# Run with debug logging
pytest tests/integration/test_deepfreeze_integration.py::TestDeepfreezeIntegration::test_thaw_complete_then_refreeze -v -s --log-cli-level=DEBUG

# Capture all logs to file
pytest tests/integration/test_deepfreeze_integration.py -v -s --log-file=test_debug.log --log-file-level=DEBUG
```

## AWS Cost Considerations

### Estimated Costs (as of 2024)

Running the full test suite incurs AWS charges:

| Resource | Usage | Estimated Cost |
|----------|-------|----------------|
| S3 Storage | ~10 GB for 1-2 days | $0.23 |
| Glacier Storage | ~10 GB for 1-2 days | $0.04 |
| Glacier Retrievals (Standard) | 3-4 retrievals × 10 GB | $0.40 |
| S3 Requests | ~1000 PUT/GET requests | $0.01 |
| **Total per full test run** | | **~$0.70** |

**Cost Reduction Tips**:
1. Use FAST_MODE for development (no AWS costs)
2. Run long tests only when necessary
3. Use smaller test data sets
4. Clean up resources promptly after tests
5. Consider using AWS Free Tier if available

### Resource Cleanup

Tests automatically clean up resources in `tearDown()`, but if tests fail:

```bash
# List S3 buckets
aws s3 ls | grep deepfreeze

# Delete test buckets
aws s3 rb s3://deepfreeze-integration-abc123 --force

# List Elasticsearch repositories
curl "localhost:9200/_snapshot/_all"

# Delete repositories
curl -X DELETE "localhost:9200/_snapshot/df-test-repo-*"
```

## Contributing

When adding new tests:

1. **Follow naming convention**: `test_<operation>_<scenario>`
2. **Add comprehensive logging**: Use `self.logger.info()` for progress
3. **Include docstring**: Describe purpose, steps, duration
4. **Clean up resources**: Implement proper `tearDown()`
5. **Add to this README**: Document runtime and requirements
6. **Test both modes**: Verify works in both FAST_MODE and real mode

### Test Template

```python
def test_new_feature(self):
    """
    Brief description of what this test validates.

    Steps:
    1. Setup phase
    2. Action phase
    3. Verification phase

    Duration: ~X hours
    """
    self.logger.info("\n" + "="*80)
    self.logger.info("TEST: New Feature")
    self.logger.info("="*80)

    # Setup
    bucket_name, repo_name = self._setup_test_environment()

    # Test logic here
    # ...

    # Verification
    self.logger.info("\n--- Verifying results ---")
    # assertions here

    self.logger.info("\n✓ Test completed successfully")
```

## Additional Resources

- [Curator Documentation](https://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html)
- [AWS Glacier Documentation](https://docs.aws.amazon.com/amazonglacier/latest/dev/introduction.html)
- [Elasticsearch Snapshot and Restore](https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-restore.html)

## Support

For issues or questions:
1. Check existing test output and logs
2. Review this README thoroughly
3. Check AWS CloudWatch logs
4. File an issue with: test name, logs, environment details
