#!/bin/bash
# NHL Data Pipeline Test Suite
#
# Usage:
#   ./run_schema_tests.sh           # Schema validation only (fast)
#   ./run_schema_tests.sh --full    # Schema + NHL API integration tests

set -e  # Exit on error

# Default: run schema validation only
TEST_MODE="schema"

# Check for --full flag
if [ "$1" = "--full" ]; then
    TEST_MODE="full"
fi

echo "========================================"
echo "NHL Data Pipeline Test Suite"
echo "========================================"
echo ""

if [ "$TEST_MODE" = "schema" ]; then
    echo "🔍 Mode: Schema Validation Only (no API calls)"
    echo "----------------------------------------"
    echo ""
    pytest tests/test_schema_simple.py -v -x
    
elif [ "$TEST_MODE" = "full" ]; then
    echo "🔍 Mode: Full Test Suite (Schema + NHL API Integration)"
    echo "----------------------------------------"
    echo ""
    
    echo "1/2 - Running Schema Validation Tests..."
    pytest tests/test_schema_simple.py -v -x
    
    echo ""
    echo "2/2 - Running NHL API Integration Tests..."
    pytest tests/test_nhl_api_integration.py -v -x
fi

echo ""
echo "========================================"
echo "✅ All tests passed! Safe to deploy."
echo "========================================"
echo ""
echo "Deploy with:"
echo "  databricks bundle deploy --profile e2-demo-field-eng"
echo ""
