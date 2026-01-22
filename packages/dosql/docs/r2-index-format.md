# R2 Single-File Index Format

A single-file index format optimized for Cloudflare R2 byte-range requests. Designed to minimize round-trips by embedding everything needed for queries in one file with offset-based access.

## Motivation

Traditional formats like Iceberg require multiple round-trips:
1. Catalog lookup
2. Manifest list read
3. Manifest files read
4. Data file metadata read

With R2's byte-range request support (500MB standard, 5GB enterprise), we can embed all metadata in a single file. Once cached, any point can be queried in 5-10ms via `Range: bytes=X-Y` headers.

## File Layout

```
+---------------------------+  Offset 0
| Header (128 bytes)        |
|   - Magic: "DOSQLIDX"     |
|   - Version: uint32       |
|   - Flags: uint32         |
|   - Section offsets       |
|   - Metadata              |
+---------------------------+  Offset 128
| Schema Section            |
|   - JSON-encoded schema   |
+---------------------------+
| Index Section             |
|   - B-tree metadata       |
|   - B-tree pages          |
+---------------------------+
| Bloom Section             |
|   - Column bloom filters  |
+---------------------------+
| Stats Section             |
|   - Global statistics     |
|   - Per-chunk statistics  |
+---------------------------+
| Data Section              |
|   - Data file pointers    |
+---------------------------+
| Footer (8 bytes)          |
|   - Magic: "DOSQLIDX"     |
+---------------------------+
```

## Header Format (128 bytes)

The fixed-size header enables single-request metadata access:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | Magic | "DOSQLIDX" |
| 8 | 4 | Version | Format version (uint32 LE) |
| 12 | 4 | Flags | Feature flags |
| 16 | 4 | Header Size | Total header size |
| 20 | 16 | Schema Ptr | offset(8) + size(4) + checksum(4) |
| 36 | 16 | Index Ptr | offset(8) + size(4) + checksum(4) |
| 52 | 16 | Bloom Ptr | offset(8) + size(4) + checksum(4) |
| 68 | 16 | Stats Ptr | offset(8) + size(4) + checksum(4) |
| 84 | 16 | Data Ptr | offset(8) + size(4) + checksum(4) |
| 100 | 8 | Total Size | File size for validation |
| 108 | 8 | Created At | Unix timestamp (ms) |
| 116 | 8 | Row Count | Total indexed rows |
| 124 | 4 | Checksum | CRC32 of header |

All multi-byte integers are little-endian.

## Feature Flags

```typescript
enum R2IndexFlags {
  NONE            = 0,
  HAS_BLOOM       = 1 << 0,  // Bloom filters present
  HAS_STATS       = 1 << 1,  // Column statistics present
  HAS_BTREE       = 1 << 2,  // B-tree indexes present
  COMPRESSED_ZSTD = 1 << 3,  // Sections compressed with zstd
  COMPRESSED_LZ4  = 1 << 4,  // Sections compressed with lz4
  HAS_CHECKSUMS   = 1 << 5,  // CRC32 checksums for sections
}
```

## Schema Section

JSON-encoded schema definition:

```json
{
  "tableName": "users",
  "columns": [
    { "name": "id", "type": "int64", "nullable": false, "ordinal": 0 },
    { "name": "name", "type": "string", "nullable": true, "ordinal": 1 },
    { "name": "created_at", "type": "timestamp", "nullable": false, "ordinal": 2 }
  ],
  "primaryKey": ["id"],
  "partitionBy": ["created_at"],
  "sortBy": [{ "name": "id", "direction": "asc" }]
}
```

### Supported Data Types

| Type | Size | Description |
|------|------|-------------|
| int8/uint8 | 1 | 8-bit integer |
| int16/uint16 | 2 | 16-bit integer |
| int32/uint32 | 4 | 32-bit integer |
| int64/uint64 | 8 | 64-bit integer |
| float32 | 4 | Single precision float |
| float64 | 8 | Double precision float |
| boolean | 1 | True/false |
| string | var | UTF-8 string |
| bytes | var | Binary data |
| timestamp | 8 | Unix timestamp (ms) |
| date | 4 | Days since epoch |
| uuid | 16 | UUID string |

## Index Section

### Structure

```
+------------------------+
| Metadata Size (4 B)    |
+------------------------+
| Metadata JSON          |
|   - Index definitions  |
|   - Page directories   |
+------------------------+
| B-tree Pages           |
|   - Page 0             |
|   - Page 1             |
|   - ...                |
+------------------------+
```

### Index Metadata

```json
{
  "indexes": [
    {
      "name": "idx_users_id",
      "columns": ["id"],
      "unique": true,
      "rootOffset": 1024,
      "pageCount": 5,
      "height": 2,
      "entryCount": "10000",
      "pageSize": 16384,
      "pageDirectory": [
        { "pageId": 0, "offset": 0, "size": 4096, "type": 2 },
        { "pageId": 1, "offset": 4096, "size": 4096, "type": 2 }
      ]
    }
  ]
}
```

### B-tree Page Format

```
+------------------------+  Offset 0
| Type (1 byte)          |  0x01=Internal, 0x02=Leaf
| Entry Count (2 bytes)  |
| Free Space (2 bytes)   |
| Next Leaf (4 bytes)    |  -1 if none
| Prev Leaf (4 bytes)    |  -1 if none
+------------------------+  Offset 13
| Key Offsets (2B each)  |
+------------------------+
| Value/Child Offsets    |
+------------------------+
| Key Data               |
+------------------------+
| Value Data             |
+------------------------+
```

## Bloom Filter Section

Bloom filters enable quick negative lookups (O(1) "definitely not here"):

```json
{
  "filters": [
    {
      "columnName": "email",
      "numHashes": 7,
      "numBits": 9585,
      "expectedElements": 1000,
      "insertedElements": 987,
      "falsePositiveRate": 0.0098,
      "bits": [255, 127, ...],
      "hashSeed": 2506447708
    }
  ]
}
```

### Optimal Parameters

For a desired false positive rate `p` and expected elements `n`:
- Bits: `m = -n * ln(p) / (ln(2)^2)`
- Hash functions: `k = (m/n) * ln(2)`

| Elements | FP Rate | Bits | Hashes | Size |
|----------|---------|------|--------|------|
| 1,000 | 1% | 9,585 | 7 | 1.2KB |
| 10,000 | 1% | 95,850 | 7 | 12KB |
| 100,000 | 1% | 958,505 | 7 | 117KB |
| 1,000,000 | 1% | 9,585,058 | 7 | 1.2MB |

## Statistics Section

### Global Statistics

```json
{
  "global": {
    "rowCount": "1000000",
    "totalBytes": "52428800",
    "chunkCount": 10,
    "columns": {
      "id": {
        "min": { "type": "int", "value": "1" },
        "max": { "type": "int", "value": "1000000" },
        "nullCount": "0",
        "distinctCount": "1000000",
        "sorted": true,
        "sortDirection": "asc"
      },
      "name": {
        "min": { "type": "string", "value": "Aaron" },
        "max": { "type": "string", "value": "Zoe" },
        "nullCount": "150",
        "distinctCount": "50000"
      }
    }
  }
}
```

### Per-Chunk Statistics

Enables partition pruning and predicate pushdown:

```json
{
  "chunks": [
    {
      "chunkId": "chunk_001",
      "rowRange": { "start": "0", "end": "100000" },
      "byteRange": { "offset": "0", "size": 5242880 },
      "columns": {
        "id": {
          "min": { "type": "int", "value": "1" },
          "max": { "type": "int", "value": "100000" }
        },
        "created_at": {
          "min": { "type": "timestamp", "value": "1704067200000" },
          "max": { "type": "timestamp", "value": "1704153600000" }
        }
      }
    }
  ]
}
```

## Data Section

References to actual data files:

```json
{
  "storageType": "external-r2",
  "totalSize": "52428800000",
  "files": [
    {
      "id": "chunk_001",
      "location": {
        "type": "r2",
        "bucket": "data-lake",
        "key": "tables/users/chunk_001.parquet"
      },
      "format": "parquet",
      "rowRange": { "start": "0", "end": "100000" },
      "size": "5242880",
      "compression": "zstd",
      "checksum": {
        "algorithm": "crc32",
        "value": "a1b2c3d4"
      }
    }
  ]
}
```

### Storage Types

| Type | Description |
|------|-------------|
| `inline` | Data embedded in index file |
| `external-r2` | Data in separate R2 objects |
| `external-url` | Data at external URLs |

### Data Formats

| Format | Description |
|--------|-------------|
| `parquet` | Apache Parquet |
| `json` | JSON array |
| `ndjson` | Newline-delimited JSON |
| `csv` | Comma-separated values |
| `avro` | Apache Avro |
| `arrow` | Apache Arrow IPC |

## Query Execution

### Single-Point Lookup

1. **Read header** (128 bytes) - Get section offsets
2. **Check bloom filter** - Quick negative lookup
3. **Read B-tree root** - Navigate to leaf
4. **Binary search** - Find exact match
5. **Return data file reference** - For actual data fetch

Typical: 2-3 range requests for point lookup.

### Range Scan

1. **Read header** - Get offsets
2. **Read statistics** - Prune chunks
3. **Navigate B-tree** - Find start leaf
4. **Scan leaves** - Follow nextLeaf chain
5. **Return matching rows**

### Predicate Evaluation

Statistics enable predicate pushdown:

| Predicate | Statistics Used |
|-----------|-----------------|
| `col = X` | min <= X <= max |
| `col < X` | min < X |
| `col > X` | max > X |
| `col BETWEEN A AND B` | Ranges overlap |
| `col IN (...)` | Any value in [min, max] |
| `col IS NULL` | nullCount > 0 |

## Usage Example

### Building an Index

```typescript
import { R2IndexWriter, createIndexWriter } from './r2-index/writer';

const writer = createIndexWriter({
  tableName: 'users',
  columns: [
    { name: 'id', type: 'int64', nullable: false, ordinal: 0 },
    { name: 'email', type: 'string', nullable: false, ordinal: 1 },
    { name: 'name', type: 'string', nullable: true, ordinal: 2 },
  ],
  primaryKey: ['id'],
  indexedColumns: ['id', 'email'],
  bloomColumns: ['email'],
});

// Add rows
for (const row of rows) {
  writer.addRow({
    id: { type: 'int', value: BigInt(row.id) },
    email: { type: 'string', value: row.email },
    name: row.name ? { type: 'string', value: row.name } : { type: 'null' },
  }, BigInt(row.id));
}

// Add data file references
writer.addDataFile({
  id: 'chunk_001',
  location: { type: 'r2', bucket: 'my-bucket', key: 'data/chunk_001.parquet' },
  format: 'parquet',
  rowRange: { start: 0n, end: 1000n },
  size: 1024n * 1024n,
});

// Build and upload
const indexBytes = writer.build();
await bucket.put('indexes/users.idx', indexBytes);
```

### Reading an Index

```typescript
import { createR2IndexReader } from './r2-index/reader';

const reader = createR2IndexReader(bucket, 'indexes/users.idx');

// Read just the header (128 bytes)
const header = await reader.getHeader();
console.log(`Rows: ${header.rowCount}`);

// Check if email exists (bloom filter)
const mightExist = await reader.bloomMightContain('email', {
  type: 'string',
  value: 'user@example.com',
});

if (mightExist) {
  // Perform actual lookup
  const result = await reader.lookup('email', {
    type: 'string',
    value: 'user@example.com',
  });

  console.log(`Found ${result.matches.length} rows`);
  console.log(`Stats: ${result.stats.rangeRequests} requests, ${result.stats.bytesRead} bytes`);
}
```

## Performance Characteristics

### Space Overhead

| Component | Typical Size |
|-----------|--------------|
| Header | 128 bytes |
| Schema | 500 bytes - 5KB |
| B-tree Index | ~10 bytes/row |
| Bloom Filter | ~1.2 bytes/row (1% FP) |
| Statistics | 200 bytes/chunk/column |
| Data Pointers | 100-500 bytes/file |

For 1M rows with 5 indexed columns:
- Header: 128 bytes
- Schema: ~1KB
- B-tree: ~50MB
- Bloom: ~6MB
- Stats: ~1MB
- **Total: ~60MB overhead**

### Latency (R2)

| Operation | Requests | Latency |
|-----------|----------|---------|
| Header read | 1 | 5-10ms |
| Bloom check | 1-2 | 5-15ms |
| Point lookup | 2-4 | 10-30ms |
| Range scan (1K rows) | 3-10 | 15-50ms |

### Caching Strategy

1. **Header**: Cache indefinitely (immutable)
2. **Bloom filters**: Cache aggressively (read-heavy)
3. **Statistics**: Cache for query planning
4. **B-tree pages**: LRU cache for hot paths

## Comparison with Alternatives

| Feature | Iceberg | Delta Lake | R2 Index |
|---------|---------|------------|----------|
| Min round-trips | 3-5 | 2-4 | 1-2 |
| Point lookup | O(n) files | O(n) files | O(log n) |
| Bloom filters | Per-file | Per-file | Global |
| Statistics | Per-file | Per-file | Unified |
| Time travel | Yes | Yes | No* |
| ACID | Yes | Yes | No* |

*Can be added via external catalog layer

## Future Extensions

1. **Compression**: Zstd/LZ4 for sections
2. **Encryption**: Per-section encryption keys
3. **Partitioning**: Multiple index files per partition
4. **MVCC**: Version chains for time travel
5. **Incremental updates**: Delta merge strategy
