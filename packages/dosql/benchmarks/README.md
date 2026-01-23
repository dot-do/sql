# DoSQL Benchmark Suite

Comprehensive benchmark suite comparing DoSQL against other SQLite implementations.

## Overview

This benchmark suite measures performance across six SQLite implementations:

| Adapter | Description | Environment |
|---------|-------------|-------------|
| **better-sqlite3** | Native synchronous SQLite | Node.js |
| **libsql** | LibSQL embedded mode | Node.js |
| **turso** | Edge SQLite via HTTP | Node.js / Workers |
| **d1** | Cloudflare D1 | Workers only |
| **do-sqlite** | Durable Object SQLite | Workers only |
| **dosql** | DoSQL implementation | Any |

## Quick Start

```bash
# Install dependencies
npm install

# Run quick benchmark (4 scenarios, 3 adapters)
npm run benchmark

# Run full benchmark suite
npm run benchmark:all

# Generate markdown report
npm run benchmark:report
```

## Benchmark Categories

### CRUD Operations
- **Simple SELECT**: Point query by primary key
- **INSERT**: Single row insertion
- **Bulk INSERT**: 1000-row batch insertion
- **UPDATE**: Single row update
- **DELETE**: Single row deletion

### Advanced Operations
- **Transaction**: Multi-statement transactions (INSERT + UPDATE + DELETE)
- **Complex JOIN**: Multi-table JOIN with aggregation

### Performance Characteristics
- **Cold Start**: Time to first query after initialization
- **Memory Usage**: Heap consumption during bulk operations

## CLI Usage

```bash
# Benchmark specific adapters
npm run benchmark -- -a dosql,libsql,better-sqlite3

# Benchmark specific scenarios
npm run benchmark -- -s insert,update,transaction

# Quick mode (reduced iterations)
npm run benchmark -- --quick

# Output JSON
npm run benchmark -- -f json -o results.json

# Output Markdown
npm run benchmark -- -f markdown -o BENCHMARKS.md

# Verbose output
npm run benchmark -- -v

# List available options
npm run benchmark -- --list
```

### All CLI Options

```
-a, --adapters <list>    Comma-separated adapters
-s, --scenarios <list>   Comma-separated scenarios
--all                    Run all comprehensive scenarios
-i, --iterations <n>     Number of iterations (default: 100)
-w, --warmup <n>         Warmup iterations (default: 10)
-f, --format <type>      Output: json, console, markdown
-o, --output <file>      Write results to file
-v, --verbose            Enable verbose logging
-q, --quick              Quick mode (reduced iterations)
--turso-url <url>        Turso database URL
--turso-token <token>    Turso auth token
-l, --list               List available adapters/scenarios
-h, --help               Show help
```

## Turso Benchmarks

To benchmark Turso, provide credentials via CLI or environment:

```bash
# Via CLI
npm run benchmark -- -a turso \
  --turso-url libsql://your-db.turso.io \
  --turso-token your-auth-token

# Via environment
export TURSO_DATABASE_URL=libsql://your-db.turso.io
export TURSO_AUTH_TOKEN=your-auth-token
npm run benchmark -- -a turso
```

## Cloudflare Worker Benchmarks

D1 and DO-SQLite adapters require Cloudflare Worker environment. These cannot run in Node.js but the adapters are provided for use in Worker-based benchmarks.

```typescript
// In a Cloudflare Worker
import { D1Adapter, DOSqliteAdapter } from '@dosql/benchmarks/adapters';

// D1
const d1Adapter = new D1Adapter({ db: env.DB });

// DO SQLite
const doAdapter = new DOSqliteAdapter({ sql: ctx.storage.sql });
```

## Programmatic Usage

```typescript
import {
  BenchmarkRunner,
  createAdapter,
  getComprehensiveScenarios,
  getDefaultScenarioConfig,
  generateMarkdownReport,
} from '@dosql/benchmarks';

// Create adapters
const adapters = await Promise.all([
  createAdapter('better-sqlite3'),
  createAdapter('libsql'),
  createAdapter('dosql'),
]);

// Get scenarios
const scenarios = getComprehensiveScenarios().map(getDefaultScenarioConfig);

// Run benchmarks
const runner = new BenchmarkRunner({ verbose: true });
const report = await runner.runComparison(adapters, scenarios);

// Generate report
const markdown = generateMarkdownReport(report);
console.log(markdown);
```

## Results Format

### JSON Output

```json
{
  "metadata": {
    "name": "SQLite Implementation Comparison Benchmark",
    "timestamp": "2026-01-23T12:00:00.000Z",
    "durationSeconds": 45.32,
    "runtime": "node"
  },
  "results": {
    "better-sqlite3": [...],
    "libsql": [...],
    "dosql": [...]
  },
  "comparisons": [...],
  "summary": {
    "totalScenarios": 8,
    "adaptersCompared": ["better-sqlite3", "libsql", "dosql"],
    "overallWinner": "better-sqlite3",
    "winsByAdapter": {...}
  }
}
```

### Metrics Collected

For each scenario:
- **Latency**: min, max, mean, median (P50), P95, P99, stdDev
- **Throughput**: ops/second, total operations, success rate
- **Memory** (where available): heap used, peak usage
- **Cold Start** (where applicable): init time, first query time

## Architecture

```
benchmarks/
├── src/
│   ├── adapters/
│   │   ├── better-sqlite3.ts  # Native SQLite
│   │   ├── libsql.ts          # LibSQL embedded
│   │   ├── turso.ts           # Turso HTTP
│   │   ├── d1.ts              # Cloudflare D1
│   │   ├── do-sqlite.ts       # DO SQLite
│   │   ├── dosql.ts           # DoSQL implementation
│   │   └── index.ts           # Adapter factory
│   ├── scenarios/
│   │   ├── simple-select.ts   # SELECT benchmarks
│   │   ├── insert.ts          # INSERT benchmarks
│   │   ├── update.ts          # UPDATE benchmarks
│   │   ├── delete.ts          # DELETE benchmarks
│   │   ├── transaction.ts     # Transaction benchmarks
│   │   ├── complex-join.ts    # JOIN benchmarks
│   │   ├── cold-start.ts      # Cold start benchmarks
│   │   ├── memory-usage.ts    # Memory benchmarks
│   │   └── index.ts           # Scenario registry
│   ├── types.ts               # Type definitions
│   ├── runner.ts              # Benchmark runner
│   ├── report.ts              # Report generator
│   ├── cli.ts                 # CLI entry point
│   └── index.ts               # Public API
├── RESULTS.md                 # Sample results
├── package.json
├── tsconfig.json
└── README.md
```

## Methodology

### Warmup Phase
Each scenario includes warmup iterations that are not counted in metrics. This ensures JIT compilation and cache warming before measurement.

### Timing
Uses `performance.now()` for sub-millisecond precision timing.

### Iterations
Default: 100 iterations per scenario (configurable). More iterations provide better statistical significance.

### Percentile Calculation
Percentiles use the rank method:
```
P(n) = sorted_timings[ceil(n/100 * count) - 1]
```

### Statistical Analysis
- **StdDev**: Lower is better (consistent performance)
- **P99/P95 ratio**: High ratio indicates tail latency issues
- **Mean vs Median**: Large difference indicates skewed distribution

## Contributing

To add a new adapter:

1. Create adapter file in `src/adapters/`
2. Implement `BenchmarkAdapter` interface
3. Add to `src/adapters/index.ts`
4. Update CLI help and README

To add a new scenario:

1. Create scenario file in `src/scenarios/`
2. Implement `ScenarioExecutor` function
3. Add to `src/scenarios/index.ts`
4. Update types if needed

## License

MIT
