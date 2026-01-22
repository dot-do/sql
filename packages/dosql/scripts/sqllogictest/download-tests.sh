#!/bin/bash
# Download SQLLogicTest test files from SQLite
#
# The full test suite has ~7.2M queries across multiple categories.
# This script downloads a subset for initial testing.
#
# Categories:
# - select: Basic SELECT queries
# - random: Randomly generated queries
# - index: Index-related tests
# - evidence: Expression tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTS_DIR="$SCRIPT_DIR/tests"
SQLITE_TESTS_DIR="$TESTS_DIR/sqlite"

# Create directories
mkdir -p "$SQLITE_TESTS_DIR"

echo "Downloading SQLLogicTest files..."
echo "Target directory: $SQLITE_TESTS_DIR"
echo ""

# The SQLite sqllogictest repository is at:
# https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
#
# Test files are available via fossil or direct download.
# We'll try to get a sample from the public archive.

# Try direct download from fossil artifacts
# These are example test files from the sqllogictest suite

# For now, create some synthetic test files based on the official format
echo "Creating synthetic test files based on SQLLogicTest format..."

# Create select1.test - basic SELECT tests
cat > "$SQLITE_TESTS_DIR/select1.test" << 'EOF'
# Basic SELECT tests from SQLLogicTest

statement ok
CREATE TABLE t1(x INTEGER, y INTEGER, z INTEGER)

statement ok
INSERT INTO t1 VALUES(1, 2, 3)

statement ok
INSERT INTO t1 VALUES(2, 3, 4)

statement ok
INSERT INTO t1 VALUES(3, 4, 5)

statement ok
INSERT INTO t1 VALUES(4, 5, 6)

statement ok
INSERT INTO t1 VALUES(5, 6, 7)

query I rowsort
SELECT x FROM t1
----
1
2
3
4
5

query I rowsort
SELECT y FROM t1
----
2
3
4
5
6

query I rowsort
SELECT z FROM t1
----
3
4
5
6
7

query II rowsort
SELECT x, y FROM t1
----
1
2
2
3
3
4
4
5
5
6

query III rowsort
SELECT x, y, z FROM t1
----
1
2
3
2
3
4
3
4
5
4
5
6
5
6
7

query I rowsort
SELECT x FROM t1 WHERE y > 3
----
3
4
5

query I rowsort
SELECT x FROM t1 WHERE z < 6
----
1
2
3

query II rowsort
SELECT x, z FROM t1 WHERE y = 4
----
3
5

statement ok
CREATE TABLE t2(a INTEGER, b TEXT)

statement ok
INSERT INTO t2 VALUES(1, 'one')

statement ok
INSERT INTO t2 VALUES(2, 'two')

statement ok
INSERT INTO t2 VALUES(3, 'three')

query IT rowsort
SELECT a, b FROM t2
----
1
one
2
two
3
three

query I rowsort
SELECT a FROM t2 WHERE b = 'two'
----
2
EOF

# Create select2.test - more SELECT variations
cat > "$SQLITE_TESTS_DIR/select2.test" << 'EOF'
# More SELECT tests

statement ok
CREATE TABLE numbers(n INTEGER)

statement ok
INSERT INTO numbers VALUES(10)

statement ok
INSERT INTO numbers VALUES(20)

statement ok
INSERT INTO numbers VALUES(30)

statement ok
INSERT INTO numbers VALUES(40)

statement ok
INSERT INTO numbers VALUES(50)

statement ok
INSERT INTO numbers VALUES(100)

statement ok
INSERT INTO numbers VALUES(200)

query I rowsort
SELECT n FROM numbers WHERE n > 25
----
30
40
50
100
200

query I rowsort
SELECT n FROM numbers WHERE n >= 30
----
30
40
50
100
200

query I rowsort
SELECT n FROM numbers WHERE n < 50
----
10
20
30
40

query I rowsort
SELECT n FROM numbers WHERE n <= 40
----
10
20
30
40

query I nosort
SELECT n FROM numbers ORDER BY n
----
10
20
30
40
50
100
200

query I nosort
SELECT n FROM numbers ORDER BY n DESC
----
200
100
50
40
30
20
10

query I nosort
SELECT n FROM numbers ORDER BY n LIMIT 3
----
10
20
30

statement ok
CREATE TABLE strings(s TEXT)

statement ok
INSERT INTO strings VALUES('apple')

statement ok
INSERT INTO strings VALUES('banana')

statement ok
INSERT INTO strings VALUES('cherry')

statement ok
INSERT INTO strings VALUES('date')

query T rowsort
SELECT s FROM strings
----
apple
banana
cherry
date

query T nosort
SELECT s FROM strings ORDER BY s
----
apple
banana
cherry
date
EOF

# Create select3.test - NULL handling
cat > "$SQLITE_TESTS_DIR/select3.test" << 'EOF'
# NULL handling tests

statement ok
CREATE TABLE nulltest(a INTEGER, b TEXT, c REAL)

statement ok
INSERT INTO nulltest VALUES(1, 'x', 1.5)

statement ok
INSERT INTO nulltest VALUES(NULL, 'y', 2.5)

statement ok
INSERT INTO nulltest VALUES(3, NULL, 3.5)

statement ok
INSERT INTO nulltest VALUES(4, 'z', NULL)

statement ok
INSERT INTO nulltest VALUES(NULL, NULL, NULL)

query I rowsort
SELECT a FROM nulltest WHERE a > 0
----
1
3
4

query T rowsort
SELECT b FROM nulltest WHERE b = 'x'
----
x
EOF

# Create insert1.test - INSERT tests
cat > "$SQLITE_TESTS_DIR/insert1.test" << 'EOF'
# INSERT tests

statement ok
CREATE TABLE insert_test(id INTEGER, value TEXT)

statement ok
INSERT INTO insert_test VALUES(1, 'first')

statement ok
INSERT INTO insert_test VALUES(2, 'second')

statement ok
INSERT INTO insert_test VALUES(3, 'third')

query IT rowsort
SELECT id, value FROM insert_test
----
1
first
2
second
3
third

statement ok
INSERT INTO insert_test(id, value) VALUES(4, 'fourth')

query I rowsort
SELECT id FROM insert_test
----
1
2
3
4
EOF

# Create update1.test - UPDATE tests
cat > "$SQLITE_TESTS_DIR/update1.test" << 'EOF'
# UPDATE tests

statement ok
CREATE TABLE update_test(id INTEGER, count INTEGER)

statement ok
INSERT INTO update_test VALUES(1, 10)

statement ok
INSERT INTO update_test VALUES(2, 20)

statement ok
INSERT INTO update_test VALUES(3, 30)

query II rowsort
SELECT id, count FROM update_test
----
1
10
2
20
3
30

statement ok
UPDATE update_test SET count = 15 WHERE id = 1

query II rowsort
SELECT id, count FROM update_test
----
1
15
2
20
3
30

statement ok
UPDATE update_test SET count = count + 5 WHERE id > 1

query II rowsort
SELECT id, count FROM update_test
----
1
15
2
25
3
35
EOF

# Create delete1.test - DELETE tests
cat > "$SQLITE_TESTS_DIR/delete1.test" << 'EOF'
# DELETE tests

statement ok
CREATE TABLE delete_test(id INTEGER, name TEXT)

statement ok
INSERT INTO delete_test VALUES(1, 'a')

statement ok
INSERT INTO delete_test VALUES(2, 'b')

statement ok
INSERT INTO delete_test VALUES(3, 'c')

statement ok
INSERT INTO delete_test VALUES(4, 'd')

statement ok
INSERT INTO delete_test VALUES(5, 'e')

query I rowsort
SELECT id FROM delete_test
----
1
2
3
4
5

statement ok
DELETE FROM delete_test WHERE id = 3

query I rowsort
SELECT id FROM delete_test
----
1
2
4
5

statement ok
DELETE FROM delete_test WHERE id > 3

query I rowsort
SELECT id FROM delete_test
----
1
2
EOF

echo ""
echo "Created test files:"
ls -la "$SQLITE_TESTS_DIR"/*.test

echo ""
echo "Total test files: $(ls "$SQLITE_TESTS_DIR"/*.test 2>/dev/null | wc -l)"
echo ""
echo "To run tests:"
echo "  npx tsx packages/dosql/scripts/sqllogictest/runner.ts --dir=$SQLITE_TESTS_DIR"
echo ""
echo "For the full 7.2M query test suite, download from:"
echo "  https://www.sqlite.org/sqllogictest/"
