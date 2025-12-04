#!/bin/bash
# Script to run deepfreeze thaw integration tests
# Usage: ./run_thaw_tests.sh [fast|full] [test_name]

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."

# Default values
MODE="${1:-fast}"
TEST_NAME="${2:-}"

# Print usage
usage() {
    echo "Usage: $0 [fast|full] [test_name]"
    echo ""
    echo "Modes:"
    echo "  fast  - Run tests with mocked operations (5-10 minutes)"
    echo "  full  - Run tests against real AWS Glacier (up to 6 hours)"
    echo ""
    echo "Examples:"
    echo "  $0 fast                                    # Run all tests in fast mode"
    echo "  $0 fast test_thaw_single_repository       # Run specific test in fast mode"
    echo "  $0 full                                    # Run all tests against real Glacier"
    echo ""
    exit 1
}

# Check if help is requested
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

# Validate mode
if [ "$MODE" != "fast" ] && [ "$MODE" != "full" ]; then
    echo -e "${RED}Error: Invalid mode '$MODE'. Must be 'fast' or 'full'${NC}"
    usage
fi

# Check for curator configuration file
CURATOR_CONFIG="${CURATOR_CONFIG:-$HOME/.curator/curator.yml}"
echo -e "${YELLOW}Checking for curator configuration...${NC}"
if [ ! -f "$CURATOR_CONFIG" ]; then
    echo -e "${RED}Error: Configuration file not found: $CURATOR_CONFIG${NC}"
    echo "Create ~/.curator/curator.yml or set CURATOR_CONFIG environment variable"
    exit 1
fi
echo -e "${GREEN}✓ Configuration file found: $CURATOR_CONFIG${NC}"

# Extract Elasticsearch host from config and check connection
echo -e "${YELLOW}Checking Elasticsearch connection from config...${NC}"
# Try to extract the host from the YAML config (simple grep approach)
ES_HOST=$(grep -A 5 "^elasticsearch:" "$CURATOR_CONFIG" | grep "hosts:" | sed 's/.*hosts: *//;s/[][]//g;s/,.*//;s/ //g' | head -1)
if [ -z "$ES_HOST" ]; then
    echo -e "${YELLOW}Warning: Could not extract Elasticsearch host from config${NC}"
    ES_HOST="http://127.0.0.1:9200"
fi

if ! curl -s "$ES_HOST" > /dev/null 2>&1; then
    echo -e "${RED}Error: Cannot connect to Elasticsearch at $ES_HOST${NC}"
    echo "Check your configuration file: $CURATOR_CONFIG"
    exit 1
fi
echo -e "${GREEN}✓ Elasticsearch is running at $ES_HOST${NC}"

# Check AWS credentials for full mode
if [ "$MODE" = "full" ]; then
    echo -e "${YELLOW}Checking AWS credentials...${NC}"
    if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        echo -e "${RED}Error: AWS credentials not found${NC}"
        echo "For full test mode, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        exit 1
    fi
    echo -e "${GREEN}✓ AWS credentials found${NC}"

    echo -e "${YELLOW}WARNING: Full test mode will take up to 6 hours to complete!${NC}"
    echo -e "${YELLOW}Press Ctrl+C within 5 seconds to cancel...${NC}"
    sleep 5
fi

# Set environment variables based on mode
if [ "$MODE" = "fast" ]; then
    export DEEPFREEZE_FAST_MODE=1
    echo -e "${GREEN}Running in FAST mode (mocked operations)${NC}"
else
    export DEEPFREEZE_FULL_TEST=1
    echo -e "${YELLOW}Running in FULL TEST mode (real AWS Glacier)${NC}"
fi

# Build test command
TEST_FILE="$SCRIPT_DIR/test_deepfreeze_thaw.py"
if [ -n "$TEST_NAME" ]; then
    TEST_PATH="$TEST_FILE::TestDeepfreezeThaw::$TEST_NAME"
    echo -e "${GREEN}Running test: $TEST_NAME${NC}"
else
    TEST_PATH="$TEST_FILE"
    echo -e "${GREEN}Running all thaw tests${NC}"
fi

# Run tests
echo -e "${YELLOW}Starting tests...${NC}"
cd "$PROJECT_ROOT"

# Run pytest with verbose output
if pytest "$TEST_PATH" -v -s --tb=short; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi
