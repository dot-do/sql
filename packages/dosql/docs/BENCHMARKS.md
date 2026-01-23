# Performance Benchmarks

> Generated: 2026-01-22T22:58:43.508Z
> Runtime: node
> Duration: 0.00s

This document contains performance benchmarks comparing DoSQL against D1 and raw DO SQLite.

## Executive Summary

- **Adapters Tested:** dosql
- **Total Scenarios:** 3
- **Overall Winner:** **DOSQL** won 3 of 3 scenarios.

### Wins by Adapter

| Adapter | Wins |
|---------|------|
| dosql | 3 |

## Summary Results

| Scenario | dosql P95 (ms) | Winner |
|----------|:-----------:|:------:|
| simple-select | 0.000 | dosql |
| insert | 0.001 | dosql |
| transaction | 0.007 | dosql |

## Latency Comparison (ms)

### simple-select

| Metric | dosql |
|--------|:------:|
| min | 0.000 |
| median | 0.000 |
| mean | 0.000 |
| p95 | 0.000 |
| p99 | 0.001 |
| max | 0.002 |
| stdDev | 0.000 |

### insert

| Metric | dosql |
|--------|:------:|
| min | 0.000 |
| median | 0.001 |
| mean | 0.001 |
| p95 | 0.001 |
| p99 | 0.003 |
| max | 0.015 |
| stdDev | 0.002 |

### transaction

| Metric | dosql |
|--------|:------:|
| min | 0.003 |
| median | 0.003 |
| mean | 0.005 |
| p95 | 0.007 |
| p99 | 0.076 |
| max | 0.076 |
| stdDev | 0.010 |


## Detailed Scenario Results
### DOSQL
#### Simple SELECT

- **Description:** Point query by primary key
- **Iterations:** 100 (warmup: 10)
- **Row Count:** 1000

**Latency:**
- P50 (Median): 0.000 ms
- P95: 0.000 ms
- P99: 0.001 ms
- Min: 0.000 ms
- Max: 0.002 ms
- Mean: 0.000 ms
- StdDev: 0.000 ms

**Throughput:**
- Ops/sec: 4088474.59
- Total Operations: 100
- Success Rate: 100.00%

#### INSERT Single Row

- **Description:** Insert a single row
- **Iterations:** 100 (warmup: 10)
- **Row Count:** 0

**Latency:**
- P50 (Median): 0.001 ms
- P95: 0.001 ms
- P99: 0.003 ms
- Min: 0.000 ms
- Max: 0.015 ms
- Mean: 0.001 ms
- StdDev: 0.002 ms

**Throughput:**
- Ops/sec: 1243394.47
- Total Operations: 100
- Success Rate: 100.00%

#### Transaction (Multi-Statement)

- **Description:** Execute multiple statements in a transaction
- **Iterations:** 50 (warmup: 5)
- **Row Count:** 100

**Latency:**
- P50 (Median): 0.003 ms
- P95: 0.007 ms
- P99: 0.076 ms
- Min: 0.003 ms
- Max: 0.076 ms
- Mean: 0.005 ms
- StdDev: 0.010 ms

**Throughput:**
- Ops/sec: 196622.03
- Total Operations: 50
- Success Rate: 100.00%


## Comparison Analysis
### simple-select

- **Winner:** dosql
- **Performance Ratio:** 1.00x

| Adapter | P95 Latency (ms) |
|---------|:----------------:|
| dosql ** | 0.000 ** |

### insert

- **Winner:** dosql
- **Performance Ratio:** 1.00x

| Adapter | P95 Latency (ms) |
|---------|:----------------:|
| dosql ** | 0.001 ** |

### transaction

- **Winner:** dosql
- **Performance Ratio:** 1.00x

| Adapter | P95 Latency (ms) |
|---------|:----------------:|
| dosql ** | 0.007 ** |


## Latency Distributions
### dosql - Simple SELECT

```
    0.00 - 0.00     | ######################################## (98)
    0.00 - 0.00     |  (0)
    0.00 - 0.00     |  (0)
    0.00 - 0.00     |  (1)
    0.00 - 0.00     |  (0)
    0.00 - 0.00     |  (0)
    0.00 - 0.00     |  (0)
    0.00 - 0.00     |  (0)
    0.00 - 0.00     |  (0)
    0.00 - 0.00     |  (1)
```

### dosql - INSERT Single Row

```
    0.00 - 0.00     | ######################################## (98)
    0.00 - 0.00     |  (1)
    0.00 - 0.00     |  (0)
    0.00 - 0.01     |  (0)
    0.01 - 0.01     |  (0)
    0.01 - 0.01     |  (0)
    0.01 - 0.01     |  (0)
    0.01 - 0.01     |  (0)
    0.01 - 0.01     |  (0)
    0.01 - 0.02     |  (1)
```

### dosql - Transaction (Multi-Statement)

```
    0.00 - 0.01     | ######################################## (48)
    0.01 - 0.02     | # (1)
    0.02 - 0.02     |  (0)
    0.02 - 0.03     |  (0)
    0.03 - 0.04     |  (0)
    0.04 - 0.05     |  (0)
    0.05 - 0.05     |  (0)
    0.05 - 0.06     |  (0)
    0.06 - 0.07     |  (0)
    0.07 - 0.08     | # (1)
```


## Methodology

### Test Environment
- **Runtime:** Cloudflare Workers / Node.js
- **Database:** SQLite (via Durable Objects / D1)

### Measurement Approach
1. **Warmup:** Each scenario begins with warmup iterations that are not counted in results
2. **Timing:** Uses `performance.now()` for high-resolution timing
3. **Iterations:** Multiple iterations per scenario for statistical significance
4. **Metrics:** P50 (median), P95, P99 latencies along with min/max/mean/stddev

### Scenarios
- **Simple SELECT:** Point query by primary key
- **INSERT:** Single row insertion
- **Transaction:** Multiple operations in a transaction
- **Batch INSERT:** Bulk data insertion

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
npm install
```

### Run Benchmarks

#### Single Adapter
```bash
# DoSQL benchmark
npm run bench:dosql

# All scenarios
npm run bench:all
```

#### Comparison Suite
```bash
# Run comparison (requires D1 and DO bindings)
npm run bench:compare
```

### CLI Options
```
-a, --adapter <name>     Adapter: dosql, d1, do-sqlite
-s, --scenario <name>    Scenario: simple-select, insert, transaction
-i, --iterations <n>     Number of iterations (default: 100)
-w, --warmup <n>         Warmup iterations (default: 10)
-f, --format <format>    Output: json, console, markdown
-v, --verbose            Enable verbose logging
```

### Environment Variables
```bash
BENCH_ITERATIONS=1000     # Override iteration count
BENCH_WARMUP=100          # Override warmup count
BENCH_OUTPUT=results.json # Save results to file
```

---

*Benchmark completed at 2026-01-22T22:58:43.508Z*

*Generated by DoSQL Benchmark Suite v1.0.0*