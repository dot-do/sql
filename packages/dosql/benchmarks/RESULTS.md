# SQLite Implementation Benchmark Results

> **Generated:** 2026-01-23T12:00:00.000Z (Sample)
> **Runtime:** node (v22.0.0)
> **Platform:** darwin
> **Duration:** 45.32s

This document contains performance benchmarks comparing DoSQL against other SQLite implementations:
- Native SQLite (better-sqlite3)
- LibSQL (local embedded)
- Turso (edge SQLite via HTTP)
- Cloudflare D1
- Durable Object SQLite

## Executive Summary

- **Adapters Tested:** SQLite (better-sqlite3), LibSQL (local), DoSQL
- **Total Scenarios:** 8
- **Overall Winner:** **SQLite (better-sqlite3)** won 6 of 8 scenarios.

### Wins by Adapter

| Adapter | Wins | Description |
|---------|:----:|-------------|
| SQLite (better-sqlite3) | 6 | Native SQLite via better-sqlite3 (synchronous, fastest) |
| LibSQL (local) | 1 | LibSQL embedded mode (async, SQLite fork with extensions) |
| DoSQL | 1 | DoSQL in-memory implementation for benchmarking |

## Summary Results (P95 Latency)

| Scenario | SQLite (better-sqlite3) (ms) | LibSQL (local) (ms) | DoSQL (ms) | Winner |
|:---------|:----------:|:----------:|:----------:|:------:|
| Simple SELECT (by PK) | **0.012** | 0.089 | 0.003 | SQLite (better-sqlite3) |
| INSERT (single row) | **0.015** | 0.142 | 0.004 | SQLite (better-sqlite3) |
| Bulk INSERT (1000 rows) | **8.234** | 45.672 | 12.345 | SQLite (better-sqlite3) |
| UPDATE (single row) | **0.018** | 0.156 | 0.005 | SQLite (better-sqlite3) |
| DELETE (single row) | **0.014** | 0.134 | 0.004 | SQLite (better-sqlite3) |
| Transaction (multi-op) | **0.245** | 0.892 | 0.089 | SQLite (better-sqlite3) |
| Complex JOIN | 2.456 | **1.234** | 0.567 | LibSQL (local) |
| Cold Start Time | 45.234 | 89.567 | **12.345** | DoSQL |

## Detailed Latency Comparison (ms)

### Simple SELECT (by PK)

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 0.008 | 0.065 | 0.001 |
| median | 0.010 | 0.078 | 0.002 |
| mean | 0.011 | 0.082 | 0.002 |
| p95 | 0.012 | 0.089 | 0.003 |
| p99 | 0.018 | 0.134 | 0.005 |
| max | 0.045 | 0.234 | 0.012 |
| stdDev | 0.003 | 0.015 | 0.001 |

### INSERT (single row)

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 0.010 | 0.098 | 0.002 |
| median | 0.012 | 0.125 | 0.003 |
| mean | 0.013 | 0.128 | 0.003 |
| p95 | 0.015 | 0.142 | 0.004 |
| p99 | 0.022 | 0.178 | 0.006 |
| max | 0.056 | 0.312 | 0.015 |
| stdDev | 0.004 | 0.018 | 0.001 |

### Bulk INSERT (1000 rows)

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 6.123 | 38.456 | 10.234 |
| median | 7.456 | 42.123 | 11.567 |
| mean | 7.678 | 43.456 | 11.892 |
| p95 | 8.234 | 45.672 | 12.345 |
| p99 | 9.123 | 52.345 | 14.567 |
| max | 12.456 | 78.234 | 18.901 |
| stdDev | 1.234 | 5.678 | 1.567 |

### UPDATE (single row)

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 0.012 | 0.112 | 0.003 |
| median | 0.015 | 0.138 | 0.004 |
| mean | 0.016 | 0.142 | 0.004 |
| p95 | 0.018 | 0.156 | 0.005 |
| p99 | 0.025 | 0.198 | 0.007 |
| max | 0.067 | 0.345 | 0.018 |
| stdDev | 0.005 | 0.021 | 0.001 |

### DELETE (single row)

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 0.009 | 0.098 | 0.002 |
| median | 0.012 | 0.118 | 0.003 |
| mean | 0.012 | 0.122 | 0.003 |
| p95 | 0.014 | 0.134 | 0.004 |
| p99 | 0.019 | 0.167 | 0.006 |
| max | 0.048 | 0.289 | 0.014 |
| stdDev | 0.003 | 0.016 | 0.001 |

### Transaction (multi-op)

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 0.178 | 0.678 | 0.056 |
| median | 0.212 | 0.789 | 0.078 |
| mean | 0.223 | 0.812 | 0.082 |
| p95 | 0.245 | 0.892 | 0.089 |
| p99 | 0.312 | 1.123 | 0.112 |
| max | 0.567 | 1.678 | 0.189 |
| stdDev | 0.045 | 0.123 | 0.018 |

### Complex JOIN

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 1.890 | 0.945 | 0.345 |
| median | 2.234 | 1.123 | 0.489 |
| mean | 2.312 | 1.156 | 0.512 |
| p95 | 2.456 | 1.234 | 0.567 |
| p99 | 3.123 | 1.567 | 0.678 |
| max | 4.567 | 2.345 | 0.912 |
| stdDev | 0.456 | 0.189 | 0.089 |

### Cold Start Time

| Metric | SQLite (better-sqlite3) | LibSQL (local) | DoSQL |
|:-------|:------:|:------:|:------:|
| min | 34.567 | 67.890 | 8.234 |
| median | 42.123 | 82.345 | 10.567 |
| mean | 43.456 | 85.123 | 11.234 |
| p95 | 45.234 | 89.567 | 12.345 |
| p99 | 52.678 | 102.345 | 15.678 |
| max | 78.901 | 145.678 | 22.345 |
| stdDev | 8.234 | 15.678 | 2.345 |

## Performance Analysis

### Simple SELECT (by PK)

- **Winner:** SQLite (better-sqlite3)
- **Performance Ratio:** 29.67x

| Adapter | P95 Latency (ms) |
|:--------|:----------------:|
| SQLite (better-sqlite3) | **0.012** |
| LibSQL (local) | 0.089 |
| DoSQL | 0.003 |

### INSERT (single row)

- **Winner:** SQLite (better-sqlite3)
- **Performance Ratio:** 35.50x

| Adapter | P95 Latency (ms) |
|:--------|:----------------:|
| SQLite (better-sqlite3) | **0.015** |
| LibSQL (local) | 0.142 |
| DoSQL | 0.004 |

### Bulk INSERT (1000 rows)

- **Winner:** SQLite (better-sqlite3)
- **Performance Ratio:** 5.55x

| Adapter | P95 Latency (ms) |
|:--------|:----------------:|
| SQLite (better-sqlite3) | **8.234** |
| LibSQL (local) | 45.672 |
| DoSQL | 12.345 |

### Transaction (multi-op)

- **Winner:** SQLite (better-sqlite3)
- **Performance Ratio:** 10.02x

| Adapter | P95 Latency (ms) |
|:--------|:----------------:|
| SQLite (better-sqlite3) | **0.245** |
| LibSQL (local) | 0.892 |
| DoSQL | 0.089 |

### Complex JOIN

- **Winner:** LibSQL (local)
- **Performance Ratio:** 2.18x

| Adapter | P95 Latency (ms) |
|:--------|:----------------:|
| SQLite (better-sqlite3) | 2.456 |
| LibSQL (local) | **1.234** |
| DoSQL | 0.567 |

### Cold Start Time

- **Winner:** DoSQL
- **Performance Ratio:** 7.26x

| Adapter | P95 Latency (ms) |
|:--------|:----------------:|
| SQLite (better-sqlite3) | 45.234 |
| LibSQL (local) | 89.567 |
| DoSQL | **12.345** |

## Key Insights

### better-sqlite3 Performance

Native SQLite via better-sqlite3 consistently wins for:
- Point queries (SELECT by PK)
- Single row mutations (INSERT, UPDATE, DELETE)
- Transaction throughput

This is expected because better-sqlite3 uses synchronous operations with no async overhead.

### LibSQL Strengths

LibSQL shows competitive performance for:
- Complex queries with JOINs
- Async-friendly workloads

LibSQL's query optimizer handles complex JOINs efficiently.

### DoSQL Advantages

DoSQL (in-memory simulation) excels at:
- Cold start time (minimal initialization)
- Simple in-memory operations

Note: DoSQL benchmarks here use an in-memory simulation. Real-world performance will vary.

### Turso Considerations

Turso (not included in this sample run) would show:
- Higher latency due to network round-trips
- Strong consistency guarantees
- Edge caching benefits for read-heavy workloads

### Cloudflare D1 / DO-SQLite

These adapters require the Cloudflare Worker runtime and aren't benchmarkable in Node.js.
Expected characteristics:
- **D1**: Optimized for edge deployment, cold starts comparable to better-sqlite3
- **DO-SQLite**: Best for transactional workloads within Durable Objects

## Methodology

### Test Environment
- **Runtime:** Node.js / Cloudflare Workers
- **Database:** SQLite-based implementations

### Measurement Approach
1. **Warmup:** Each scenario begins with warmup iterations (not counted)
2. **Timing:** Uses `performance.now()` for high-resolution timing
3. **Iterations:** Multiple iterations per scenario for statistical significance
4. **Metrics:** P50, P95, P99 latencies along with min/max/mean/stddev

### Scenarios Tested
- **Simple SELECT:** Point query by primary key
- **INSERT:** Single row insertion
- **Bulk INSERT:** 1000-row batch insertion
- **UPDATE:** Single row update
- **DELETE:** Single row deletion
- **Transaction:** Multiple operations in a transaction
- **Complex JOIN:** Multi-table JOIN with aggregation
- **Cold Start:** Time to first query after initialization

### Percentile Calculation
Percentiles are calculated using the rank method:
- Sort all timings
- P(n) = timing at position ceil(n/100 * count)

### Statistical Notes
- Standard deviation indicates timing consistency
- High P99/P95 ratio suggests occasional slow queries
- Compare mean vs median to detect skew

## Reproduction

### Prerequisites
```bash
cd packages/dosql
npm install
```

### Run Benchmarks

#### Quick Benchmark (4 scenarios)
```bash
npm run benchmark
```

#### Full Benchmark Suite
```bash
npm run benchmark:all
```

#### Specific Adapters/Scenarios
```bash
npm run benchmark -- -a dosql,libsql -s insert,update,transaction
```

#### Output Formats
```bash
# JSON output
npm run benchmark -- -f json -o results.json

# Markdown report
npm run benchmark -- -f markdown -o BENCHMARKS.md
```

#### With Turso
```bash
npm run benchmark -- -a turso \
  --turso-url libsql://your-db.turso.io \
  --turso-token your-token
```

### CLI Options
```
-a, --adapters <list>    Comma-separated adapters
-s, --scenarios <list>   Comma-separated scenarios
--all                    Run all scenarios
-i, --iterations <n>     Number of iterations
-w, --warmup <n>         Warmup iterations
-f, --format <type>      Output: json, console, markdown
-o, --output <file>      Write results to file
-v, --verbose            Enable verbose logging
-q, --quick              Quick mode (reduced iterations)
-l, --list               List available adapters/scenarios
```

### Environment Variables
```bash
TURSO_DATABASE_URL=libsql://...
TURSO_AUTH_TOKEN=...
```

---

*Benchmark completed at 2026-01-23T12:00:00.000Z (Sample)*

*Generated by DoSQL Benchmark Suite v1.0.0*
