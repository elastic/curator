# Deepfreeze Thaw Integration Tests

This document describes the integration tests for deepfreeze thaw operations.

## Overview

The thaw integration tests (`test_deepfreeze_thaw.py`) verify the complete lifecycle of thawing repositories from Glacier storage, including:

1. Creating thaw requests with specific date ranges
2. Monitoring restore progress using porcelain output
3. Verifying indices are mounted correctly after restoration
4. Verifying data can be searched in mounted indices
5. Running cleanup operations
6. Verifying repositories are unmounted after cleanup

## Test Modes

These tests support two modes of operation:

### Fast Mode (Development/CI)

Fast mode uses mocked operations to complete quickly, suitable for CI/CD pipelines.

```bash
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_thaw.py -v
```

**Duration**: ~5-10 minutes per test
**Use case**: Local development, CI/CD, quick verification

**What's mocked:**
- Glacier restore operations (instant completion)
- S3 object restoration progress
- Time-based expiration (accelerated)

### Full Test Mode (Production Validation)

Full test mode runs against real AWS Glacier, taking up to 6 hours for complete restoration.

```bash
DEEPFREEZE_FULL_TEST=1 pytest tests/integration/test_deepfreeze_thaw.py -v
```

**Duration**: Up to 6 hours per test (depending on AWS Glacier restore tier)
**Use case**: Pre-release validation, production readiness testing

**Requirements:**
- Valid AWS credentials configured
- S3 bucket access
- Glacier restore permissions
- Elasticsearch instance with snapshot repository support

## Test Suite

### Test Cases

#### 1. `test_thaw_single_repository`

Tests thawing a single repository containing data for a specific date range.

**What it tests:**
- Creating test indices with timestamped data
- Snapshotting indices to a repository
- Pushing repository to Glacier
- Creating a thaw request for a specific date range
- Monitoring restore progress using porcelain output
- Verifying correct indices are mounted
- Verifying data is searchable
- Refreezing the repository

**Date Range:** January 2024 (single month)
**Expected Result:** 1 repository thawed and mounted

#### 2. `test_thaw_multiple_repositories`

Tests thawing multiple repositories spanning a date range.

**What it tests:**
- Creating multiple repositories via rotation
- Creating test data across multiple time periods
- Pushing all repositories to Glacier
- Creating a thaw request spanning multiple repositories
- Verifying all relevant repositories are restored
- Verifying repositories outside the date range are NOT thawed
- Searching data across multiple thawed repositories

**Date Range:** January-February 2024 (two months)
**Expected Result:** 2 repositories thawed, 1 repository remains frozen

#### 3. `test_thaw_with_porcelain_output_parsing`

Tests the porcelain output format and parsing logic.

**What it tests:**
- Porcelain output format from thaw commands
- Parsing REQUEST and REPO lines
- Checking restore completion status
- Monitoring repository mount status
- Progress tracking (0/100, Complete, etc.)

**Output Format:**
```
REQUEST	{request_id}	{status}	{created_at}	{start_date}	{end_date}
REPO	{name}	{bucket}	{path}	{state}	{mounted}	{progress}
```

#### 4. `test_cleanup_removes_expired_repositories`

Tests automatic cleanup of expired thaw requests.

**What it tests:**
- Creating a thaw request with short duration
- Manually expiring the request
- Running cleanup operation
- Verifying repositories are unmounted
- Verifying thaw state is reset to frozen
- Verifying thaw request is marked as completed

**Duration:** 1 day (manually expired for testing)

## Running the Tests

### Prerequisites

1. **Curator Configuration File**

   The tests use the configuration from `~/.curator/curator.yml` by default.

   Create the configuration file if it doesn't exist:
   ```bash
   mkdir -p ~/.curator
   cat > ~/.curator/curator.yml <<EOF
   ---
   elasticsearch:
     client:
       hosts: http://localhost:9200
       request_timeout: 30
     other_settings:
       master_only: False

   logging:
     loglevel: DEBUG
     logfile:
     logformat: default
     blacklist: ["urllib3", "elastic_transport"]
   EOF
   ```

   Or use a custom configuration file:
   ```bash
   export CURATOR_CONFIG=/path/to/your/curator.yml
   ```

2. **AWS Credentials** (for full test mode)
   ```bash
   export AWS_ACCESS_KEY_ID="your-access-key"
   export AWS_SECRET_ACCESS_KEY="your-secret-key"
   export AWS_DEFAULT_REGION="us-west-2"
   ```

3. **Python Dependencies**
   ```bash
   pip install -e .
   pip install pytest pytest-cov
   ```

### Run All Thaw Tests (Fast Mode)

```bash
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_thaw.py -v -s
```

### Run Specific Test

```bash
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_thaw.py::TestDeepfreezeThaw::test_thaw_single_repository -v -s
```

### Run with Coverage

```bash
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_thaw.py --cov=curator.actions.deepfreeze --cov-report=term-missing
```

### Run Full Integration Tests

**WARNING:** This will take several hours to complete!

```bash
DEEPFREEZE_FULL_TEST=1 pytest tests/integration/test_deepfreeze_thaw.py -v -s
```

## Porcelain Output Parser

The `ThawStatusParser` class provides utilities for parsing machine-readable output:

```python
from tests.integration.test_deepfreeze_thaw import ThawStatusParser

# Parse status output
parser = ThawStatusParser()
status_data = parser.parse_status_output(output_string)

# Check if restore is complete
is_complete = parser.is_restore_complete(status_data)

# Check if all repos are mounted
all_mounted = parser.all_repos_mounted(status_data)
```

## Test Data Structure

Each test creates indices with realistic timestamped data:

```python
# Example: Creating test data for January 2024
date_ranges = [
    (datetime(2024, 1, 1), datetime(2024, 1, 31))
]
indices = self._create_test_indices_with_dates(repo_name, date_ranges)

# Creates index: test-logs-20240101-000
# With 100 documents spanning the date range
# Each document has @timestamp field for querying
```

## Expected Test Output

### Fast Mode Success

```
test_deepfreeze_thaw.py::TestDeepfreezeThaw::test_thaw_single_repository PASSED
test_deepfreeze_thaw.py::TestDeepfreezeThaw::test_thaw_multiple_repositories PASSED
test_deepfreeze_thaw.py::TestDeepfreezeThaw::test_thaw_with_porcelain_output_parsing PASSED
test_deepfreeze_thaw.py::TestDeepfreezeThaw::test_cleanup_removes_expired_repositories PASSED

4 passed in 15.23s
```

### Full Test Mode (Example)

```
test_deepfreeze_thaw.py::TestDeepfreezeThaw::test_thaw_single_repository
Creating test data...
Pushing repository to Glacier...
Creating thaw request...
Waiting for restore (this may take up to 6 hours)...
[Progress updates every 30 seconds]
Restore completed after 4h 23m
Verifying indices are mounted...
Verifying data can be searched...
PASSED [18943.45s]
```

## Troubleshooting

### Test Skipped

If you see:
```
SKIPPED [1] test_deepfreeze_thaw.py:42: Thaw tests are long-running. Set DEEPFREEZE_FULL_TEST=1 or DEEPFREEZE_FAST_MODE=1 to run.
```

Solution: Set one of the environment variables:
```bash
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_thaw.py
```

### AWS Credentials Error

If you see authentication errors in full test mode:
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

Solution: Configure AWS credentials:
```bash
aws configure
# or
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```

### Configuration File Not Found

If you see:
```
SKIPPED - Configuration file not found: /Users/username/.curator/curator.yml
```

Solution: Create the configuration file:
```bash
mkdir -p ~/.curator
cat > ~/.curator/curator.yml <<EOF
---
elasticsearch:
  client:
    hosts: http://localhost:9200
    request_timeout: 30
  other_settings:
    master_only: False

logging:
  loglevel: DEBUG
  logfile:
  logformat: default
  blacklist: ["urllib3", "elastic_transport"]
EOF
```

Or point to an existing configuration:
```bash
export CURATOR_CONFIG=/path/to/your/curator.yml
```

### Elasticsearch Connection Error

If tests fail to connect to Elasticsearch:
```
elasticsearch8.exceptions.ConnectionError: Connection refused
```

Solution: Start Elasticsearch and update your curator.yml:
```bash
# Start Elasticsearch (example)
docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:8.12.0

# Verify connection
curl http://localhost:9200

# Update curator.yml with the correct host
vi ~/.curator/curator.yml
# Edit the hosts: line to point to your Elasticsearch instance
```

### Restore Timeout

If tests timeout waiting for restore:
```
AssertionError: Restore did not complete within timeout period
```

In full test mode, this is expected if:
- AWS Glacier is taking longer than usual
- Network issues are slowing the restore
- The retrieval tier is set to "Bulk" (12-48 hours)

Solution:
- Use "Expedited" tier for faster restore (1-5 minutes)
- Increase timeout in test code
- Use FAST_MODE for development

## Continuous Integration

For CI pipelines, use fast mode:

```yaml
# GitHub Actions example
- name: Run Deepfreeze Thaw Tests
  env:
    DEEPFREEZE_FAST_MODE: "1"
    TEST_ES_SERVER: "http://localhost:9200"
  run: |
    pytest tests/integration/test_deepfreeze_thaw.py -v
```

## Contributing

When adding new thaw-related tests:

1. Follow the existing test patterns
2. Use the `ThawStatusParser` for porcelain output parsing
3. Support both FAST_MODE and FULL_TEST modes
4. Add appropriate assertions for data verification
5. Clean up test resources in tearDown
6. Document expected behavior and timing

## Related Documentation

- [Deepfreeze How It Works](./DEEPFREEZE_HOW_IT_WORKS.md)
- [Thaw Action Source](../../curator/actions/deepfreeze/thaw.py)
- [Cleanup Action Source](../../curator/actions/deepfreeze/cleanup.py)
- [Refreeze Action Source](../../curator/actions/deepfreeze/refreeze.py)
