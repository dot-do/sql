# SQLLogicTest Runner for DoSQL

> **Pre-release Software**: This is v0.1.0. APIs may change. Not recommended for production use without thorough testing.

A standalone test runner that executes [SQLLogicTest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) test files against the DoSQL engine.

## Stability

### Stable APIs

- Test file parsing (SQLLogicTest format)
- Basic test execution
- Result comparison and reporting

### Experimental APIs

- Full SQLLogicTest compatibility (some edge cases may not be supported)
- Custom DoSQL extensions (`skipif dosql`, `onlyif dosql`)

## Version Compatibility

| Dependency | Version |
|------------|---------|
| Node.js | 18+ |
| TypeScript | 5.3+ |
| tsx | 4.x |

## Overview

SQLLogicTest is a comprehensive SQL testing framework developed by the SQLite project. It contains millions of test cases designed to verify SQL engine correctness. This runner implements the SQLLogicTest parser and executor for DoSQL.

## Quick Start

```bash
# Run all tests with default settings
npx tsx packages/dosql/scripts/sqllogictest/runner.ts

# Run with a limit (for quick checks)
npx tsx packages/dosql/scripts/sqllogictest/runner.ts --limit=1000

# Run a specific test file
npx tsx packages/dosql/scripts/sqllogictest/runner.ts --file=tests/sqlite/select1.test

# Run with verbose output
npx tsx packages/dosql/scripts/sqllogictest/runner.ts --verbose

# Run tests in a directory
npx tsx packages/dosql/scripts/sqllogictest/runner.ts --dir=tests/sqlite
```

## Test File Format

SQLLogicTest uses a simple text format with the following record types:

### Statement Records

Execute SQL commands that should succeed or fail:

```
statement ok
CREATE TABLE t1(a INTEGER, b TEXT)

statement error
INSERT INTO nonexistent VALUES(1)
```

### Query Records

Execute SQL queries and verify results:

```
query ITR rowsort
SELECT id, name, score FROM users
----
1
Alice
95.5
2
Bob
87.0
```

- **Type string**: Column types (I=integer, T=text, R=real)
- **Sort mode**: `nosort`, `rowsort`, or `valuesort`
- **Label** (optional): For result reuse

### Control Records

```
hash-threshold 20    # Use MD5 hash for results > 20 values
halt                 # Stop processing
skipif dosql         # Skip for DoSQL
onlyif dosql         # Only run for DoSQL
```

## Current Baseline Results

**Date**: 2025-01-21
**DoSQL Version**: 0.1.0-rc.1

```
============================================================
SQLLOGICTEST RESULTS
============================================================

Total Tests:    115
Passed:         112 (97.39%)
Failed:         3
Skipped:        0

Results by Category:
--------------------------------------------------
  basic              25 /    26 passed (96.2%)
  delete             11 /    11 passed (100.0%)
  insert              7 /     7 passed (100.0%)
  sample             12 /    12 passed (100.0%)
  select             48 /    50 passed (96.0%)
  update              9 /     9 passed (100.0%)
============================================================
```

## Known Issues

### 1. >= and <= Comparison Operators (3 failures)

The DoSQL InMemoryEngine has a regex parsing issue where `>=` and `<=` operators may not be correctly matched. The regex pattern matches `>` before `>=` due to alternation order.

**Affected queries**:
- `SELECT ... WHERE col >= value`
- `SELECT ... WHERE col <= value`

**Location**: `packages/dosql/src/statement/statement.ts` (filterRows method)

### 2. INSERT Shorthand Syntax (documented)

DoSQL requires explicit column names in INSERT statements:

```sql
-- Works
INSERT INTO t1(a, b, c) VALUES(1, 2, 3)

-- Does NOT work
INSERT INTO t1 VALUES(1, 2, 3)
```

## File Structure

```
packages/dosql/scripts/sqllogictest/
├── README.md           # This file
├── runner.ts           # Main test runner
├── parser.ts           # SQLLogicTest format parser
├── download-tests.sh   # Script to create test files
└── tests/
    ├── sample.test     # Sample tests
    ├── basic.test      # Basic CRUD tests
    └── sqlite/         # SQLite-style tests
        ├── select1.test
        ├── select2.test
        ├── select3.test
        ├── insert1.test
        ├── update1.test
        └── delete1.test
```

## Parser API

```typescript
import { parseTestFile, shouldSkip, formatValue, sortValues } from './parser.js';

// Parse a test file
const content = fs.readFileSync('test.test', 'utf-8');
const { records, errors, stats } = parseTestFile(content);

// Check if a record should be skipped
if (shouldSkip(record, 'dosql')) {
  // Skip this test
}

// Format values for comparison
const formatted = formatValue(123.456, 'R');  // "123.456"
const formattedNull = formatValue(null, 'T'); // "NULL"

// Sort values according to sort mode
const sorted = sortValues(values, 'rowsort', columnCount);
```

## Getting More Tests

The full SQLLogicTest suite contains approximately 7.2 million test queries. To download more tests:

1. Visit https://www.sqlite.org/sqllogictest/
2. Download the test archive
3. Extract to `tests/sqlite-full/`

```bash
# Run subset of full tests
npx tsx runner.ts --dir=tests/sqlite-full --limit=10000
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--file=PATH` | Run a specific test file |
| `--dir=PATH` | Run all .test files in directory |
| `--limit=N` | Run only first N tests |
| `--verbose` | Show detailed output per test |
| `--continue` | Continue on error (default: true) |
| `--stop-on-error` | Stop at first failure |
| `--help` | Show help message |

## Exit Codes

- `0` - All tests passed
- `1` - One or more tests failed

## Contributing

To add new test files:

1. Create a `.test` file in the `tests/` directory
2. Follow the SQLLogicTest format specification
3. Run the tests to verify

### Test Writing Guidelines

1. **Use explicit column names in INSERT**:
   ```sql
   INSERT INTO t1(a, b) VALUES(1, 2)
   ```

2. **Use rowsort for non-deterministic ordering**:
   ```
   query I rowsort
   SELECT x FROM t1
   ```

3. **Use nosort only with ORDER BY or single rows**:
   ```
   query I nosort
   SELECT x FROM t1 ORDER BY x
   ```

4. **Values are one per line**:
   ```
   ----
   1
   2
   3
   ```

## References

- [SQLLogicTest Documentation](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki)
- [DoSQL Package](../../README.md)
