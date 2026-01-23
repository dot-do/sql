# DoSQL Benchmark Suite

Benchmark suite for comparing **DoSQL** against **D1** and **DO-SQLite** (Durable Object SQLite).

## Overview

This benchmark suite provides:

- **Standardized scenarios** for comparing SQL performance
- **Adapter implementations** for DoSQL, D1, and DO-SQLite
- **CLI runner** for quick benchmarks
- **JSON output** for integration with CI/CD

## Installation

```bash
# From the dosql package root
cd packages/dosql/benchmark
npm install
```

## Quick Start

Run benchmarks from the command line:

```bash
# Simple SELECT benchmark
npx tsx src/cli.ts -a dosql -s simple-select

# INSERT benchmark with verbose output
npx tsx src/cli.ts -a dosql -s insert -v

# Transaction benchmark with 500 iterations
npx tsx src/cli.ts -a dosql -s transaction -i 500 -f console
```

## CLI Options

```
Usage: npx tsx src/cli.ts [options]

Options:
  -a, --adapter <name>     Adapter to benchmark (dosql, d1, do-sqlite)
  -s, --scenario <name>    Scenario to run (simple-select, insert, transaction)
  -i, --iterations <n>     Number of iterations (default: 100)
  -w, --warmup <n>         Number of warmup iterations (default: 10)
  -f, --format <format>    Output format: json or console (default: json)
  -v, --verbose            Enable verbose logging
  -h, --help               Show this help message
```

## Scenarios

### Simple SELECT

Point query by primary key - the most common read pattern.

```sql
SELECT * FROM table WHERE id = ?
```

### INSERT

Single-row insert - tests write latency.

```sql
INSERT INTO table (id, name, value, data, created_at) VALUES (?, ?, ?, ?, ?)
```

### Transaction

Multi-statement transaction combining INSERT, UPDATE, and DELETE.

```sql
BEGIN TRANSACTION;
INSERT INTO table VALUES (...);
UPDATE table SET ... WHERE id = ?;
DELETE FROM table WHERE id = ?;
COMMIT;
```

## Output Format

### JSON Output (default)

```json
{
  "adapter": "dosql",
  "scenario": "simple-select",
  "latency": {
    "min": 0.042,
    "max": 1.523,
    "mean": 0.089,
    "median": 0.076,
    "p95": 0.156,
    "p99": 0.312,
    "stdDev": 0.045
  },
  "throughput": {
    "opsPerSecond": 11235.82,
    "successRate": 1.0
  },
  "timestamp": "2026-01-22T12:00:00.000Z",
  "durationSeconds": 1.234
}
```

### Console Output

```
=== Benchmark Results ===

Adapter:   dosql
Scenario:  simple-select
Duration:  1.23s

Latency (ms):
  Min:    0.042
  Max:    1.523
  Mean:   0.089
  Median: 0.076
  P95:    0.156
  P99:    0.312
  StdDev: 0.045

Throughput:
  Ops/sec:      11235.82
  Success Rate: 100.00%
```

## Programmatic Usage

```typescript
import { BenchmarkRunner, DoSQLAdapter, createSimpleSelectScenario } from 'dosql-benchmark';

// Create adapter and runner
const adapter = new DoSQLAdapter();
const runner = new BenchmarkRunner({ verbose: true });

// Configure scenario
const scenario = createSimpleSelectScenario({
  iterations: 1000,
  warmupIterations: 100,
  rowCount: 10000,
});

// Run benchmark
const result = await runner.runScenario(adapter, scenario);

console.log(`P95 Latency: ${result.latency.p95.toFixed(2)}ms`);
console.log(`Throughput: ${result.throughput.opsPerSecond.toFixed(2)} ops/sec`);
```

## Adapters

### DoSQL Adapter

In-memory SQL engine for testing and development.

```typescript
import { DoSQLAdapter } from 'dosql-benchmark';

const adapter = new DoSQLAdapter();
```

### D1 Adapter

Cloudflare D1 database (requires Worker environment).

```typescript
import { D1Adapter } from 'dosql-benchmark';

// In a Worker
const adapter = new D1Adapter({ db: env.DB });
```

### DO-SQLite Adapter

Durable Object SQLite (requires Worker environment).

```typescript
import { DOSqliteAdapter } from 'dosql-benchmark';

// In a Durable Object
const adapter = new DOSqliteAdapter({ storage: ctx.storage });
```

## Running Comparisons

For comparing adapters (requires Worker environment for D1/DO-SQLite):

```typescript
import {
  BenchmarkRunner,
  DoSQLAdapter,
  D1Adapter,
  DOSqliteAdapter,
  DEFAULT_SCENARIOS,
} from 'dosql-benchmark';

const runner = new BenchmarkRunner({ verbose: true });

const adapters = [
  new DoSQLAdapter(),
  new D1Adapter({ db: env.DB }),
  new DOSqliteAdapter({ storage: ctx.storage }),
];

const report = await runner.runComparison(adapters, DEFAULT_SCENARIOS);

console.log('Overall Winner:', report.summary.overallWinner);
console.log('Wins by Adapter:', report.summary.winsByAdapter);
```

## License

MIT
