# DoSQL Bundle Size Analysis

Generated: 2026-01-21

## Executive Summary

This analysis examines the bundle size of DoSQL when compiled for Cloudflare Workers using esbuild.

**Key Finding**: The DoSQL Worker bundle is extremely compact at **7.36 KB gzipped**, using only **0.7%** of the free tier 1MB limit, leaving ample room for application code.

### Bundle Size Summary

| Bundle | Unminified | Minified | Gzipped | CF Free (1MB) | CF Paid (10MB) |
|--------|------------|----------|---------|---------------|----------------|
| DoSQL Worker | 49.24 KB | 24.13 KB | **7.36 KB** | 0.7% | 0.1% |
| DoSQL Database API | 39.9 KB | 17.41 KB | **5.94 KB** | 0.6% | 0.1% |
| DoSQL Full Library | 230.46 KB | 111.8 KB | **34.26 KB** | 3.3% | 0.3% |

### External Dependency Sizes

| Package | Unminified | Minified | Gzipped | Workers Compatible |
|---------|------------|----------|---------|-------------------|
| capnweb | 75.0 KB | 34.4 KB | **10.08 KB** | Yes |
| iceberg-js | 16.6 KB | 5.9 KB | **1.82 KB** | Yes |
| ai-evaluate | N/A | N/A | N/A | No (Node.js only) |

## Cloudflare Worker Limits

| Tier | Compressed Limit | DoSQL Worker Usage | Remaining |
|------|------------------|-------------------|-----------|
| Free | 1 MB | 7.36 KB (0.7%) | **1016.64 KB** |
| Paid | 10 MB | 7.36 KB (0.1%) | **9.99 MB** |

## Detailed Bundle Analysis

### DoSQL Worker Bundle

**Entry Point**: `src/worker/index.ts`

**Description**: Minimal worker bundle containing only the essential storage primitives for a Durable Object-based SQL database.

#### Size Metrics

- Unminified: **49.24 KB**
- Minified: **24.13 KB**
- Gzipped: **7.36 KB**

#### Module Breakdown

| Category | Size | % of Bundle | Files | Description |
|----------|------|-------------|-------|-------------|
| dosql/btree | 19.05 KB | 38.7% | 4 | B+tree implementation for row storage |
| dosql/worker | 13.94 KB | 28.3% | 2 | Worker entrypoint and DO class |
| dosql/fsx | 8.89 KB | 18.0% | 5 | File system abstraction (DO/R2 backends) |
| dosql/wal | 7.03 KB | 14.3% | 3 | Write-ahead log for durability |

**Analysis**: The worker bundle is lean and focused. The B-tree module is the largest component (38.7%), which is expected as it's the core data structure. No external dependencies are included.

---

### DoSQL Database API Bundle

**Entry Point**: `src/database.ts`

**Description**: High-level database API providing better-sqlite3/D1 compatible interface.

#### Size Metrics

- Unminified: **39.9 KB**
- Minified: **17.41 KB**
- Gzipped: **5.94 KB**

#### Module Breakdown

| Category | Size | % of Bundle | Files |
|----------|------|-------------|-------|
| dosql/core | 39.52 KB | 99.0% | 4 |

**Analysis**: This bundle provides the Database class with prepared statements, transactions, and pragma support without including storage primitives.

---

### DoSQL Full Library Bundle

**Entry Point**: `src/index.ts`

**Description**: Complete library with all modules including sharding, procedures, CDC, and virtual tables.

#### Size Metrics

- Unminified: **230.46 KB**
- Minified: **111.8 KB**
- Gzipped: **34.26 KB**

#### Module Breakdown

| Category | Size | % of Bundle | Files | Description |
|----------|------|-------------|-------|-------------|
| dosql/sharding | 51.88 KB | 22.5% | 6 | Multi-DO sharding support |
| dosql/proc | 42.64 KB | 18.5% | 5 | ESM stored procedures |
| dosql/sources | 30.91 KB | 13.4% | 5 | URL table sources (HTTP, S3, etc.) |
| dosql/wal | 28.9 KB | 12.5% | 5 | WAL with extended features |
| dosql/transaction | 27.9 KB | 12.1% | 4 | MVCC transactions |
| dosql/virtual | 21.55 KB | 9.4% | 3 | Virtual table support |
| dosql/parser | 11.84 KB | 5.1% | 2 | SQL parser utilities |
| dosql/cdc | 10.98 KB | 4.8% | 3 | Change data capture |

**Analysis**: The full library is larger but still well within limits. Sharding and procedures are the largest modules and can be tree-shaken if not used.

---

## External Dependencies Analysis

### capnweb (RPC Library)

| Metric | Value |
|--------|-------|
| Unminified | 75.0 KB |
| Minified | 34.4 KB |
| Gzipped | 10.08 KB |
| Workers Compatible | Yes |

**Impact**: Adding capnweb to the worker bundle would increase gzipped size by ~10 KB (from 7.36 KB to ~17 KB).

### iceberg-js (Iceberg REST Client)

| Metric | Value |
|--------|-------|
| Unminified | 16.6 KB |
| Minified | 5.9 KB |
| Gzipped | 1.82 KB |
| Workers Compatible | Yes |

**Impact**: Adding iceberg-js to the worker bundle would increase gzipped size by ~2 KB (from 7.36 KB to ~9 KB).

### ai-evaluate (Code Sandbox)

| Metric | Value |
|--------|-------|
| Bundle Size | Cannot bundle |
| Workers Compatible | **No** |

**Reason**: Uses Node.js-specific modules (fs, child_process, crypto). Must be excluded from worker bundles.

---

## Theoretical Maximum Bundle

If all optional components were included:

| Component | Gzipped Size |
|-----------|-------------|
| DoSQL Full Library | 34.26 KB |
| capnweb | 10.08 KB |
| iceberg-js | 1.82 KB |
| **Total** | **~46 KB** |

**Conclusion**: Even with all dependencies, the bundle would use only **4.5%** of the free tier limit.

---

## Bundle Size Reduction Opportunities

### 1. Module-Level Code Splitting

The full library can be split into lazy-loaded chunks:

| Module | Current Size | Load Strategy |
|--------|-------------|---------------|
| dosql/sharding | 51.88 KB | On-demand when multi-DO needed |
| dosql/proc | 42.64 KB | When stored procedures used |
| dosql/sources | 30.91 KB | When URL tables created |
| dosql/virtual | 21.55 KB | When virtual tables used |
| dosql/cdc | 10.98 KB | When CDC streams created |

**Potential Savings**: Up to 70% reduction in initial bundle if using dynamic imports.

### 2. Tree Shaking Improvements

Current issues that may prevent optimal tree shaking:

```typescript
// Current (prevents tree shaking)
export * from './sharding/index.js';

// Recommended (explicit exports)
export { createShardRouter, ShardRouter } from './sharding/router.js';
```

**Recommendation**: Add `"sideEffects": false` to package.json:

```json
{
  "sideEffects": false
}
```

### 3. Dead Code Elimination

Use esbuild's `define` to eliminate development-only code:

```bash
npx esbuild --define:DEBUG=false --define:process.env.NODE_ENV=\"production\"
```

### 4. Alternative Dependency Strategies

| Current | Alternative | Savings |
|---------|-------------|---------|
| capnweb (full) | capnweb/core | ~5 KB |
| iceberg-js | Inline minimal client | ~1 KB |
| Full parser | Minimal SQL subset | TBD |

---

## Comparison with Similar Projects

| Project | Gzipped Size | Notes |
|---------|-------------|-------|
| **DoSQL Worker** | **7.36 KB** | This project |
| better-sqlite3-wasm | ~800 KB | Full SQLite in WASM |
| sql.js | ~500 KB | SQLite compiled to JS |
| absurd-sql | ~400 KB | SQLite with IndexedDB |
| PGLite | ~3 MB | PostgreSQL in WASM |
| DuckDB-wasm | ~4 MB | DuckDB in WASM |

**Advantage**: DoSQL achieves SQL-like functionality at 1-2% the size of WASM-based alternatives by using native JS data structures instead of a compiled database engine.

---

## Recommendations

### For Production Deployments

1. **Use the worker bundle** (`src/worker/index.ts`) as the entry point
2. **Exclude ai-evaluate** from worker builds (Node.js only)
3. **Lazy-load optional modules** (sharding, procedures, CDC)

### For Development

1. **Run `node scripts/bundle-analysis.mjs`** after significant changes
2. **Monitor gzipped size** - it's what Cloudflare counts
3. **Test with `wrangler publish --dry-run`** to verify actual deploy size

### For CI/CD

```bash
# Check bundle size doesn't exceed threshold
BUNDLE_SIZE=$(gzip -c dist-bundle/worker.min.js | wc -c)
if [ $BUNDLE_SIZE -gt 51200 ]; then  # 50KB threshold
  echo "Warning: Bundle size ($BUNDLE_SIZE bytes) exceeds 50KB"
  exit 1
fi
```

---

## Build Commands

```bash
# Generate this analysis
node scripts/bundle-analysis.mjs

# Build worker bundle (production)
npx esbuild src/worker/index.ts \
  --bundle \
  --minify \
  --format=esm \
  --target=es2022 \
  --platform=browser \
  --external:cloudflare:* \
  --outfile=dist-bundle/worker.min.js

# Build with source map
npx esbuild src/worker/index.ts \
  --bundle \
  --minify \
  --sourcemap \
  --format=esm \
  --outfile=dist-bundle/worker.min.js

# Analyze bundle composition
npx esbuild src/worker/index.ts \
  --bundle \
  --metafile=meta.json \
  --outfile=dist-bundle/worker.js
npx esbuild --analyze=verbose < meta.json

# Check gzipped size
gzip -c dist-bundle/worker.min.js | wc -c
```

---

## Appendix: Bundle File Locations

| File | Description |
|------|-------------|
| `dist-bundle-analysis/worker.js` | Unminified worker bundle |
| `dist-bundle-analysis/worker.min.js` | Minified worker bundle |
| `dist-bundle-analysis/full.js` | Unminified full library |
| `dist-bundle-analysis/full.min.js` | Minified full library |
| `dist-bundle-analysis/bundle-analysis.json` | Raw analysis data |
| `scripts/bundle-analysis.mjs` | Analysis script |
