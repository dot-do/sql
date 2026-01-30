#!/bin/bash
# Run the full SQLLogicTest suite against DoSQL
#
# This script runs test files individually to avoid memory issues,
# and aggregates results at the end.
#
# Usage:
#   ./run-full-suite.sh                    # Run official SQLite tests
#   ./run-full-suite.sh --quick            # Quick run with 1000 test limit
#   ./run-full-suite.sh --dir=tests/sqlite # Run specific directory
#   ./run-full-suite.sh --full             # Run entire sqlite-full suite (slow!)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default settings
TEST_DIR="tests/official"
LIMIT=""
VERBOSE=""
NODE_HEAP="4096"  # 4GB default

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --quick)
      LIMIT="--limit=1000"
      shift
      ;;
    --full)
      TEST_DIR="tests/sqlite-full"
      NODE_HEAP="8192"
      shift
      ;;
    --dir=*)
      TEST_DIR="${1#*=}"
      shift
      ;;
    --limit=*)
      LIMIT="--limit=${1#*=}"
      shift
      ;;
    --verbose)
      VERBOSE="--verbose"
      shift
      ;;
    --heap=*)
      NODE_HEAP="${1#*=}"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--quick] [--full] [--dir=PATH] [--limit=N] [--verbose] [--heap=MB]"
      exit 1
      ;;
  esac
done

# Results file
RESULTS_FILE="/tmp/sqllogictest-results-$(date +%Y%m%d-%H%M%S).txt"
SUMMARY_FILE="/tmp/sqllogictest-summary-$(date +%Y%m%d-%H%M%S).txt"

echo "============================================================"
echo "SQLLogicTest Full Suite Runner"
echo "============================================================"
echo ""
echo "Test directory: $TEST_DIR"
echo "Node heap size: ${NODE_HEAP}MB"
echo "Limit: ${LIMIT:-none}"
echo "Results: $RESULTS_FILE"
echo ""

# Find all test files
if [[ -d "$TEST_DIR" ]]; then
  TEST_FILES=$(find "$TEST_DIR" -name "*.test" -type f | sort)
else
  echo "Error: Test directory not found: $TEST_DIR"
  exit 1
fi

FILE_COUNT=$(echo "$TEST_FILES" | wc -l | tr -d ' ')
echo "Found $FILE_COUNT test files"
echo ""

# Initialize counters
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_SKIPPED=0
FILES_PROCESSED=0

# Run each test file
for TEST_FILE in $TEST_FILES; do
  FILES_PROCESSED=$((FILES_PROCESSED + 1))
  BASENAME=$(basename "$TEST_FILE")

  echo -n "[$FILES_PROCESSED/$FILE_COUNT] Running $BASENAME... "

  # Run the test with memory limit and capture output
  OUTPUT=$(NODE_OPTIONS="--max-old-space-size=$NODE_HEAP" npx tsx runner.ts \
    --file="$TEST_FILE" \
    $LIMIT \
    --continue \
    2>&1) || true

  # Extract pass/fail counts from output
  PASSED=$(echo "$OUTPUT" | grep -E "^Passed:" | head -1 | awk '{print $2}' || echo "0")
  FAILED=$(echo "$OUTPUT" | grep -E "^Failed:" | head -1 | awk '{print $2}' || echo "0")
  SKIPPED=$(echo "$OUTPUT" | grep -E "^Skipped:" | head -1 | awk '{print $2}' || echo "0")

  # Handle empty values
  PASSED=${PASSED:-0}
  FAILED=${FAILED:-0}
  SKIPPED=${SKIPPED:-0}

  # Accumulate totals
  TOTAL_PASSED=$((TOTAL_PASSED + PASSED))
  TOTAL_FAILED=$((TOTAL_FAILED + FAILED))
  TOTAL_SKIPPED=$((TOTAL_SKIPPED + SKIPPED))

  # Show inline result
  if [[ "$FAILED" == "0" ]]; then
    echo "✓ $PASSED passed"
  else
    echo "✗ $PASSED passed, $FAILED failed"
  fi

  # Save detailed output
  echo "=== $TEST_FILE ===" >> "$RESULTS_FILE"
  echo "$OUTPUT" >> "$RESULTS_FILE"
  echo "" >> "$RESULTS_FILE"

  # Memory pressure - clear node cache periodically
  if [[ $((FILES_PROCESSED % 10)) -eq 0 ]]; then
    # Brief pause to allow GC
    sleep 0.5
  fi
done

# Calculate totals
TOTAL_TESTS=$((TOTAL_PASSED + TOTAL_FAILED + TOTAL_SKIPPED))
if [[ $TOTAL_TESTS -gt 0 ]]; then
  PASS_RATE=$(echo "scale=2; $TOTAL_PASSED * 100 / $TOTAL_TESTS" | bc)
else
  PASS_RATE="0"
fi

# Print summary
echo ""
echo "============================================================"
echo "FINAL RESULTS"
echo "============================================================"
echo ""
echo "Files Processed: $FILES_PROCESSED"
echo "Total Tests:     $TOTAL_TESTS"
echo "Passed:          $TOTAL_PASSED ($PASS_RATE%)"
echo "Failed:          $TOTAL_FAILED"
echo "Skipped:         $TOTAL_SKIPPED"
echo ""
echo "Detailed results: $RESULTS_FILE"
echo "============================================================"

# Save summary
cat > "$SUMMARY_FILE" << EOF
SQLLogicTest Results Summary
Date: $(date)
Test Directory: $TEST_DIR

Files Processed: $FILES_PROCESSED
Total Tests:     $TOTAL_TESTS
Passed:          $TOTAL_PASSED ($PASS_RATE%)
Failed:          $TOTAL_FAILED
Skipped:         $TOTAL_SKIPPED
EOF

echo "Summary saved: $SUMMARY_FILE"

# Exit with error if any tests failed
if [[ $TOTAL_FAILED -gt 0 ]]; then
  exit 1
fi
