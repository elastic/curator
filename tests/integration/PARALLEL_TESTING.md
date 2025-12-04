# Parallel Integration Testing with pytest-xdist

The deepfreeze integration tests support parallel execution using pytest-xdist, allowing multiple tests to run simultaneously against real AWS S3/Glacier infrastructure.

## Benefits of Parallel Execution

**Time Savings**: When running tests that each take 6 hours sequentially, the total time is `6 hours × N tests`. With parallel execution, the total time is approximately `6 hours` (the duration of the longest test), regardless of how many tests run in parallel.

**Example**:
- Sequential: 4 tests × 6 hours = **24 hours total**
- Parallel (4 workers): max(6h, 6h, 6h, 6h) = **6 hours total**

## Installation

### 1. Install pytest-xdist

```bash
# Install test dependencies including pytest-xdist
pip install -e ".[test]"

# Or install directly
pip install pytest-xdist filelock
```

### 2. Verify Installation

```bash
pytest --version
# Should show pytest-xdist plugin
```

## Running Tests in Parallel

### Basic Parallel Execution

```bash
# Run with 4 parallel workers
pytest tests/integration/test_deepfreeze_integration.py -n 4 -v

# Run with auto-detect number of CPUs
pytest tests/integration/test_deepfreeze_integration.py -n auto -v

# Run specific tests in parallel
pytest tests/integration/test_deepfreeze_integration.py -n 2 \
  -k "thaw_complete or multiple_concurrent" -v
```

### Combining with Other Options

```bash
# Parallel with FAST_MODE for development
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_integration.py -n 4 -v

# Parallel with real AWS operations (long-running)
pytest tests/integration/test_deepfreeze_integration.py -n 3 -v -s

# Parallel with coverage
pytest tests/integration/test_deepfreeze_integration.py -n 4 -v \
  --cov=curator.actions.deepfreeze --cov-report=term-missing

# Skip long tests but run in parallel
DEEPFREEZE_SKIP_LONG_TESTS=1 pytest tests/integration/test_deepfreeze_integration.py -n 4 -v
```

## How Repository Locking Works

### Automatic Conflict Prevention

The test suite uses distributed locking to prevent parallel tests from operating on the same repository:

1. **Lock Acquisition**: Before using a repository, tests acquire an exclusive lock via Elasticsearch
2. **Automatic Filtering**: `_get_repos_not_in_active_requests()` automatically excludes locked repositories
3. **Lock Release**: Locks are automatically released in `tearDown()`
4. **Expiration**: Locks expire after 2 hours to prevent deadlocks from crashed tests

### Lock Index

Locks are stored in the `.deepfreeze_test_locks` index with:
- **repo_name**: Repository being locked (document ID)
- **locked_by**: Test ID that owns the lock
- **locked_at**: Timestamp when locked
- **expires_at**: When lock expires (2 hours)

### Expired Lock Cleanup

The test suite automatically cleans up expired locks at startup:

```python
@classmethod
def setUpClass(cls):
    """Clean up expired locks before starting test suite"""
    cleanup_expired_locks(builder.client)
```

You can also manually clean up locks:

```bash
# Delete all test locks
curl -X DELETE "http://localhost:9200/.deepfreeze_test_locks"
```

## Optimal Worker Count

### For Real AWS Operations (6-hour tests)

```bash
# Use number of available repositories
# If you have 10 frozen repositories, use 10 workers
pytest tests/integration/test_deepfreeze_integration.py -n 10 -v
```

**Key Insight**: More workers don't increase speed for long-running AWS operations. The bottleneck is Glacier restore time (1-6 hours), not CPU. Use `N workers` where `N = number of available frozen repositories` to maximize parallelism.

### For FAST_MODE (simulated operations)

```bash
# Use CPU count for fast tests
pytest tests/integration/test_deepfreeze_integration.py -n auto -v
```

FAST_MODE tests are CPU-bound, so more workers = faster execution up to the CPU count.

## Viewing Parallel Test Output

### Live Output

```bash
# Show live output from all workers
pytest tests/integration/test_deepfreeze_integration.py -n 4 -v -s
```

### Per-Worker Logging

pytest-xdist automatically captures output per worker. Use pytest's built-in output capture:

```bash
# Show full output on failures
pytest tests/integration/test_deepfreeze_integration.py -n 4 -v --tb=long

# Show all output
pytest tests/integration/test_deepfreeze_integration.py -n 4 -v -rA
```

### Test Progress Dashboard

```bash
# Install pytest-xdist with dashboard support
pip install pytest-xdist pytest-html

# Generate HTML report
pytest tests/integration/test_deepfreeze_integration.py -n 4 -v \
  --html=report.html --self-contained-html
```

## Troubleshooting

### Problem: Tests Skip Due to No Available Repositories

```
pytest.skip("Need at least 3 frozen repositories not in active requests")
```

**Cause**: Not enough unlocked repositories available

**Solutions**:
1. Reduce worker count: `-n 2` instead of `-n 4`
2. Wait for other tests to complete and release locks
3. Create more frozen repositories using `curator_cli deepfreeze setup` + `rotate`
4. Clean up expired locks manually

### Problem: Tests Timeout Waiting for Lock

```
WARNING: Failed to acquire lock on repository deepfreeze-000005 after 30 seconds
```

**Cause**: Repository locked by another test or orphaned lock

**Solutions**:
1. Wait for the other test to complete
2. Check for hung tests and kill them
3. Delete the specific lock:
   ```bash
   curl -X DELETE "http://localhost:9200/.deepfreeze_test_locks/deepfreeze-000005"
   ```

### Problem: Test Conflicts Despite Locking

**Check lock index**:
```bash
# View all current locks
curl "http://localhost:9200/.deepfreeze_test_locks/_search?pretty"
```

**Verify locks are being acquired**:
```bash
# Watch lock creation in real-time
watch -n 1 'curl -s "http://localhost:9200/.deepfreeze_test_locks/_search?size=100" | jq ".hits.hits[]._source"'
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deepfreeze Integration Tests (Parallel)

on:
  schedule:
    - cron: '0 2 * * 0'  # Weekly Sunday 2am

jobs:
  parallel-integration-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 450  # 7.5 hours
    strategy:
      matrix:
        # Run different test groups in parallel GitHub jobs
        test-group:
          - test_new_thaw_request_full_lifecycle
          - test_thaw_complete_then_refreeze
          - test_multiple_concurrent_thaw_requests
          - test_cleanup_mixed_expiration_states
      fail-fast: false  # Continue other tests if one fails

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

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

      - name: Install dependencies
        run: pip install -e ".[test]"

      - name: Run test group
        run: |
          pytest tests/integration/test_deepfreeze_integration.py \
            -k "${{ matrix.test-group }}" -v -s

      - name: Upload logs on failure
        if: failure()
        uses: actions/upload-artifact@v3
        with:
          name: test-logs-${{ matrix.test-group }}
          path: |
            /var/log/curator/
            ./test-results/
```

### Local Parallel Execution for CI Testing

```bash
# Simulate CI environment locally
pip install -e ".[test]"

# Run tests that CI would run
DEEPFREEZE_SKIP_LONG_TESTS=1 pytest tests/integration/test_deepfreeze_integration.py -n 4 -v

# Or with FAST_MODE
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_integration.py -n auto -v
```

## Best Practices

1. **Start Small**: Begin with `-n 2` to verify locking works, then increase
2. **Match Workers to Resources**: Use `N workers = available frozen repositories`
3. **Monitor Lock Index**: Check `.deepfreeze_test_locks` if tests mysteriously skip
4. **Clean Between Runs**: Delete lock index between test runs: `curl -X DELETE "http://localhost:9200/.deepfreeze_test_locks"`
5. **Use FAST_MODE for Development**: Real AWS operations take hours, use FAST_MODE for rapid iteration
6. **Set Timeouts**: Use pytest's `--timeout` to prevent hung tests from holding locks forever

## Performance Comparison

### Sequential Execution
```bash
# 4 tests × 6 hours each = 24 hours total
pytest tests/integration/test_deepfreeze_integration.py -v
```

**Total Time**: ~24 hours

### Parallel Execution
```bash
# 4 tests × 6 hours each ÷ 4 workers = 6 hours total
pytest tests/integration/test_deepfreeze_integration.py -n 4 -v
```

**Total Time**: ~6 hours (75% time reduction!)

### FAST_MODE Parallel
```bash
# Simulated operations, CPU-bound
DEEPFREEZE_FAST_MODE=1 pytest tests/integration/test_deepfreeze_integration.py -n auto -v
```

**Total Time**: ~15-20 minutes (vs ~60 minutes sequential)

## Additional Resources

- [pytest-xdist Documentation](https://pytest-xdist.readthedocs.io/)
- [Curator Deepfreeze Documentation](../../curator/actions/deepfreeze/docs/)
- [Integration Test README](./README_DEEPFREEZE.md)
