# SQL Benchmark Datasets Reference

This document catalogs well-known benchmark datasets for SQL database performance testing, covering sizes from 100MB to 10GB with realistic relational schemas.

## Table of Contents

1. [Standard Benchmarks (TPC)](#standard-benchmarks-tpc)
2. [Real-World Datasets](#real-world-datasets)
3. [Domain-Specific Datasets](#domain-specific-datasets)
4. [Summary Comparison](#summary-comparison)

---

## Standard Benchmarks (TPC)

### TPC-H (Decision Support)

**Purpose:** OLAP/analytical workload benchmark simulating ad-hoc decision support queries.

| Scale Factor | Data Size | Lineitem Rows | Use Case |
|-------------|-----------|---------------|----------|
| SF=1 | ~1 GB | 6M | Development/CI |
| SF=10 | ~10 GB | 60M | Small benchmark |
| SF=100 | ~100 GB | 600M | Production benchmark |
| SF=1000 | ~1 TB | 6B | Large-scale testing |

**Schema:** 8 tables with foreign key relationships
- `lineitem` (fact table, largest)
- `orders`, `customer`, `part`, `supplier`, `partsupp`, `nation`, `region`

**Query Types:** 22 analytical queries covering:
- Aggregations with GROUP BY
- Multi-table joins (up to 8 tables)
- Subqueries and correlated subqueries
- Date range filtering

**Download Options:**
```bash
# Using DuckDB (easiest)
duckdb -c "CALL dbgen(sf = 1);"

# Using tpch-kit
git clone https://github.com/gregrahn/tpch-kit.git
cd tpch-kit/dbgen && make
./dbgen -s 1  # generates SF=1

# Pre-generated from S3 (ClickHouse)
# See: https://clickhouse.com/docs/getting-started/example-datasets/tpch
```

**Sources:**
- [TPC-H Official](https://www.tpc.org/tpch/)
- [DuckDB TPC-H Extension](https://duckdb.org/docs/stable/core_extensions/tpch)
- [ClickHouse TPC-H Guide](https://clickhouse.com/docs/getting-started/example-datasets/tpch)

---

### TPC-DS (Decision Support - Complex)

**Purpose:** More complex OLAP benchmark modeling a retail product supplier with multiple fact tables.

| Scale Factor | Data Size | Store Sales Rows | Use Case |
|-------------|-----------|------------------|----------|
| SF=1 | ~1 GB | 2.8M | Development |
| SF=10 | ~10 GB | 28M | Small benchmark |
| SF=100 | ~100 GB | 287M | Production benchmark |
| SF=1000 | ~1 TB | 2.9B | Large-scale testing |

**Schema:** 24 tables including:
- 7 fact tables: `store_sales`, `store_returns`, `catalog_sales`, `catalog_returns`, `web_sales`, `web_returns`, `inventory`
- 17 dimension tables: `customer`, `item`, `store`, `warehouse`, `promotion`, etc.

**Query Types:** 99 queries covering:
- Complex multi-fact-table joins
- Reporting queries
- OLAP window functions
- Ad-hoc analysis patterns

**Download Options:**
```bash
# Databricks (pre-loaded)
# samples.tpcds.tpcds_sf1 and samples.tpcds.tpcds_sf1000

# Using dsdgen (requires TPC membership for official tool)
# Community alternatives exist on GitHub
```

**Sources:**
- [TPC-DS Official](https://www.tpc.org/tpcds/)
- [Databricks TPC-DS Guide](https://docs.databricks.com/aws/en/sql/tpcds-eval)
- [Snowflake TPC-DS](https://docs.snowflake.com/en/user-guide/sample-data-tpcds)

---

### TPC-C (OLTP)

**Purpose:** OLTP benchmark simulating order-entry system with concurrent transactions.

| Warehouses | Data Size | Typical Use |
|------------|-----------|-------------|
| 10 | ~1 GB | Development |
| 100 | ~10 GB | Small production |
| 1000 | ~100 GB | Production |
| 10000 | ~1 TB | Large-scale |

**Schema:** 9 tables
- `warehouse`, `district`, `customer`, `history`, `new_order`, `order`, `order_line`, `item`, `stock`

**Transaction Types:** 5 transaction profiles
- New Order (45%)
- Payment (43%)
- Order Status (4%)
- Delivery (4%)
- Stock Level (4%)

**Generation Tools:**
```bash
# BenchBase (successor to OLTP-Bench)
git clone https://github.com/cmu-db/benchbase.git
# Configure and run TPC-C workload

# CockroachDB workload generator
cockroach workload init tpcc --warehouses=10 'postgresql://...'
```

**Sources:**
- [TPC-C Official](https://www.tpc.org/tpcc/)
- [BenchBase GitHub](https://github.com/cmu-db/benchbase)
- [CockroachDB TPC-C Guide](https://github.com/cockroachdb/docs/blob/master/v19.2/performance-benchmarking-with-tpc-c-10-warehouses.md)

---

### SSB (Star Schema Benchmark)

**Purpose:** Simplified star-schema benchmark derived from TPC-H, easier to understand and run.

| Scale Factor | Data Size | Lineorder Rows |
|-------------|-----------|----------------|
| SF=1 | ~600 MB | 6M |
| SF=10 | ~6 GB | 60M |
| SF=100 | ~60 GB | 600M |

**Schema:** 5 tables (true star schema)
- `lineorder` (fact table)
- `customer`, `part`, `supplier`, `date` (dimensions)

**Query Types:** 13 queries in 4 query flights
- Q1: Filtering and aggregation
- Q2: Two-dimension joins
- Q3: Three-dimension joins
- Q4: Four-dimension joins

**Download Options:**
```bash
# ssb-dbgen
git clone https://github.com/vadimtk/ssb-dbgen.git
cd ssb-dbgen && make
./dbgen -s 1 -T a  # generates all tables at SF=1
```

**Sources:**
- [SSB Original Paper (PDF)](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF)
- [ClickHouse SSB Guide](https://clickhouse.com/docs/getting-started/example-datasets/star-schema)
- [ssb-dbgen GitHub](https://github.com/vadimtk/ssb-dbgen)

---

## Real-World Datasets

### NYC Taxi Trip Data

**Purpose:** Real-world analytics benchmark with 1.1+ billion taxi trips from 2009-present.

| Subset | Data Size | Rows | Use Case |
|--------|-----------|------|----------|
| 1 month | ~2 GB | ~12M | Quick tests |
| 1 year | ~25 GB | ~150M | Standard benchmark |
| Full (2009-2016) | ~105 GB | 1.1B | Large-scale |

**Schema:** Flat table with 17-24 columns
- `pickup_datetime`, `dropoff_datetime`
- `pickup_latitude/longitude`, `dropoff_latitude/longitude`
- `passenger_count`, `trip_distance`
- `fare_amount`, `tip_amount`, `total_amount`
- `payment_type`, `rate_code`

**Query Types:**
- Time-series aggregations (trips per hour/day/month)
- Geographic analysis (pickup/dropoff hotspots)
- Fare analysis and tip percentage calculations
- Seasonal pattern detection

**Download Options:**
```bash
# Official NYC TLC (Parquet format, recommended)
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# ClickHouse pre-processed
# https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi

# Direct Parquet files
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

**Sources:**
- [NYC TLC Official](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Mark Litwintschik's Benchmark](https://tech.marksblogg.com/billion-nyc-taxi-rides-sqlite-parquet-hdfs.html)
- [ClickHouse NYC Taxi](https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi)
- [GitHub: nyc-taxi-data](https://github.com/toddwschneider/nyc-taxi-data)

---

### Stack Overflow Database

**Purpose:** Real-world Q&A platform data with realistic distributions and diverse data types.

| Version | Data Size | Posts | Use Case |
|---------|-----------|-------|----------|
| Mini (2008-2009) | ~1.5 GB | ~1M | Development |
| 2010 | ~10 GB | ~5M | Medium benchmark |
| 2018/06 | ~180 GB | ~40M | Large benchmark |
| Full (2024) | ~200 GB | ~52M | Production testing |

**Schema:** 8+ tables
- `Posts` (questions & answers, 52M+ rows)
- `Users` (18M+ rows)
- `Votes` (208M+ rows)
- `Comments`, `Badges`, `PostLinks`, `PostHistory`, `Tags`

**Query Types:**
- User activity analysis
- Tag popularity trends
- Answer quality scoring
- Community growth patterns

**Download Options:**
```bash
# Mini version (GitHub, SQL Server 2022 format)
# https://github.com/BrentOzarULTD/Stack-Overflow-Database

# Full version (BitTorrent)
# https://www.brentozar.com/archive/2015/10/how-to-download-the-stack-overflow-database-via-bittorrent/

# PostgreSQL version
# https://smartpostgres.com/posts/announcing-early-access-to-the-stack-overflow-sample-database-download-for-postgres/
```

**Sources:**
- [Brent Ozar's Guide](https://www.brentozar.com/archive/2015/10/how-to-download-the-stack-overflow-database-via-bittorrent/)
- [Internet Archive (2024-04)](https://archive.org/details/stack-overflow-data-dump-2024-04-microsoft-sql-server-database)
- [PostgreSQL Version](https://smartpostgres.com/posts/announcing-early-access-to-the-stack-overflow-sample-database-download-for-postgres/)

---

### ClickBench (Web Analytics)

**Purpose:** Real-world web analytics data from a major analytics platform, anonymized but preserving distributions.

| Format | Size | Rows |
|--------|------|------|
| TSV.gz (compressed) | 16 GB | 100M |
| Parquet | 14.8 GB | 100M |
| Uncompressed | 70 GB | 100M |

**Schema:** Single wide table with 105 columns
- Web analytics metrics (pageviews, sessions)
- User attributes (browser, OS, device)
- Geographic data (country, region, city)
- Referrer and UTM parameters

**Query Types:** 43 queries covering:
- Simple aggregations
- Filtering with multiple conditions
- GROUP BY with various cardinalities
- String operations and pattern matching

**Download Options:**
```bash
# TSV format
curl -O https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz

# Parquet format
curl -O https://datasets.clickhouse.com/hits_compatible/hits.parquet

# Partitioned Parquet (100 files)
# See ClickBench GitHub for details
```

**Sources:**
- [ClickBench Official](https://benchmark.clickhouse.com/)
- [ClickBench GitHub](https://github.com/ClickHouse/ClickBench)
- [CedarDB ClickBench Tutorial](https://cedardb.com/docs/cookbook/clickbench/)

---

### GitHub Archive

**Purpose:** Public GitHub event data for analyzing open source activity.

| Timeframe | Data Size | Events |
|-----------|-----------|--------|
| 1 day | ~2-5 GB | ~5M |
| 1 month | ~60-150 GB | ~150M |
| Full archive | 3+ TB | Billions |

**Schema:** Event-based with nested JSON
- `type` (PushEvent, PullRequestEvent, IssuesEvent, etc.)
- `actor` (user info)
- `repo` (repository info)
- `payload` (event-specific data)
- `created_at`

**Query Types:**
- Repository popularity trends
- Developer activity patterns
- Language usage statistics
- Contribution analysis

**Download Options:**
```bash
# Hourly archives (JSON, gzipped)
curl -O https://data.gharchive.org/2024-01-01-0.json.gz

# BigQuery (1TB free/month)
# Project: githubarchive.day or githubarchive.month

# Via GH Archive website
# https://www.gharchive.org/
```

**Sources:**
- [GH Archive](https://www.gharchive.org/)
- [BigQuery GitHub Dataset](https://github.com/igrigorik/gharchive.org/tree/master/bigquery)
- [Google Codelabs Tutorial](https://codelabs.developers.google.com/codelabs/bigquery-github)

---

### Airline On-Time Performance

**Purpose:** US domestic flight performance data from 1987-present.

| Timeframe | Data Size | Flights |
|-----------|-----------|---------|
| 1 year | ~500 MB | ~6M |
| 10 years | ~5 GB | ~60M |
| Full (1987-2024) | ~15 GB | 150M+ |

**Schema:** ~30 columns
- Flight identifiers (carrier, flight number, tail number)
- Origin/destination airports
- Scheduled and actual times
- Delay causes and durations
- Cancellation/diversion info

**Query Types:**
- Route performance analysis
- Carrier reliability comparisons
- Delay pattern analysis
- Seasonal trends

**Download Options:**
```bash
# Bureau of Transportation Statistics
# https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236

# Kaggle
# https://www.kaggle.com/datasets/bulter22/airline-data

# Custom date range selection available on BTS website
```

**Sources:**
- [BTS On-Time Statistics](https://www.transtats.bts.gov/ontime/)
- [BTS Data Download](https://www.transtats.bts.gov/Tables.asp?DB_ID=120)
- [Kaggle Airline Data](https://www.kaggle.com/datasets/bulter22/airline-data)

---

### JOB (Join Order Benchmark)

**Purpose:** Complex join benchmark based on IMDB data, designed to stress query optimizers.

| Component | Size |
|-----------|------|
| Compressed | 1.2 GB |
| Uncompressed | 3.7 GB |
| Tables | 21 |

**Schema:** 21 tables from IMDB
- `title`, `movie_info`, `movie_info_idx`
- `cast_info`, `name`, `char_name`
- `company_name`, `company_type`, `movie_companies`
- `keyword`, `movie_keyword`
- And more relationship tables

**Query Types:** 113 queries with:
- 3-16 joins per query (average: 8)
- Real-world correlations in data
- Challenging cardinality estimation
- Various selectivities

**Download Options:**
```bash
# CedarDB mirror (recommended)
curl -OL https://bonsai.cedardb.com/job/imdb.tgz

# Original paper dataset
curl -O http://homepages.cwi.nl/~boncz/job/imdb.tgz

# Harvard Dataverse
# https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/2QYZBT
```

**Sources:**
- [JOB GitHub](https://github.com/gregrahn/join-order-benchmark)
- [CedarDB JOB Guide](https://cedardb.com/docs/example_datasets/job/)
- [Original Paper](https://www.researchgate.net/publication/319893076_Query_optimization_through_the_looking_glass)

---

## Domain-Specific Datasets

### E-Commerce (TPC-DS Based)

TPC-DS effectively models e-commerce scenarios with:
- Customer demographics and purchase history
- Product catalog with categories
- Multiple sales channels (store, catalog, web)
- Returns and inventory management
- Promotional campaigns

See [TPC-DS section](#tpc-ds-decision-support---complex) above.

**Additional Resources:**
- [SQL Habit E-commerce Dataset](https://www.sqlhabit.com/datasets/ecommerce) - Simplified Amazon-like schema
- [Firebolt E-commerce Sample](https://www.firebolt.io/free-sample-datasets/e-commerce) - Event tracking focused

---

### Financial Tick Data

**Purpose:** High-frequency trading data for time-series analysis.

| Source | Data Volume | Notes |
|--------|-------------|-------|
| Databento | 16 PB+ | Commercial, nanosecond precision |
| FirstRate Data | 2.4 TB (compressed) | Historical tick-by-tick |
| TimescaleDB Tutorial | ~8M rows | Free crypto tick data |

**Schema (typical):**
- `timestamp` (nanosecond precision)
- `symbol`, `exchange`
- `price`, `size`
- `bid`, `ask`, `bid_size`, `ask_size`

**Query Types:**
- VWAP calculations
- Time-weighted aggregations
- Gap analysis
- Volatility calculations

**Sources:**
- [TimescaleDB Financial Tutorial](https://docs.timescale.com/tutorials/latest/financial-tick-data/financial-tick-dataset/)
- [QuestDB Tick Data Examples](https://questdb.com/glossary/tick-data/)
- [GitHub: Financial Time Series](https://github.com/TheSnowGuru/Stocks-Futures-Financial-Time-series-Tick-Bar-Data)

---

### IoT Sensor Data

**Purpose:** Time-series sensor readings for IoT analytics.

| Dataset | Size | Sensors | Notes |
|---------|------|---------|-------|
| TSBS IoT | Configurable | 3600 devices | Benchmark suite |
| ZC-train (Subway) | Medium | 4705 measurements | Real industrial |
| ZY-machine (Tobacco) | Medium | 117 indices | Production logs |

**Schema (TSBS example):**
- `time`, `device_id`, `host_id`
- `temperature`, `humidity`, `pressure`
- `battery_level`, `signal_strength`

**Query Types:**
- Downsampling and aggregation
- Anomaly detection queries
- Device health monitoring
- Fill gaps and interpolation

**Generation Tools:**
```bash
# Time Series Benchmark Suite
git clone https://github.com/timescale/tsbs.git
# Generate IoT data with configurable parameters

# TimescaleDB simulation
# See: https://www.tigerdata.com/blog/how-to-explore-timescaledb-using-simulated-iot-sensor-data
```

**Sources:**
- [TSBS GitHub](https://github.com/timescale/tsbs)
- [Apache IoTDB Benchmark](https://iotdb.apache.org/UserGuide/latest-Table/Tools-System/Benchmark.html)
- [Kaggle IoT Time Series](https://www.kaggle.com/datasets/vetrirah/ml-iot)

---

### Log Analytics

**Purpose:** Web server access logs for log analysis benchmarking.

| Dataset | Size | Records | Format |
|---------|------|---------|--------|
| 10M Nginx Logs | ~1 GB | 10M | Standard nginx format |
| Loghub Apache | Varies | Varies | Error logs |

**Schema (nginx access log):**
- `remote_addr`, `time_local`
- `request`, `status`, `body_bytes_sent`
- `http_referer`, `http_user_agent`

**Query Types:**
- Status code distribution
- Top URLs by traffic
- Error rate analysis
- Bot detection

**Sources:**
- [Kaggle Web Server Logs](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs)
- [db-benchmarks.com 10M Logs](https://db-benchmarks.com/test-logs10m/)
- [Loghub Collection](https://arxiv.org/pdf/2008.06448)

---

## Summary Comparison

### By Workload Type

| Dataset | OLTP | OLAP | Mixed | Time-Series |
|---------|:----:|:----:|:-----:|:-----------:|
| TPC-C | Yes | - | - | - |
| TPC-H | - | Yes | - | - |
| TPC-DS | - | Yes | - | - |
| SSB | - | Yes | - | - |
| NYC Taxi | - | Yes | - | Yes |
| Stack Overflow | - | - | Yes | - |
| ClickBench | - | Yes | - | - |
| JOB | - | - | Yes | - |
| Airline | - | Yes | - | Yes |
| IoT/TSBS | - | - | - | Yes |

### By Target Size (100MB - 10GB)

| Dataset | Configuration | Size |
|---------|---------------|------|
| TPC-H SF=1 | Default | ~1 GB |
| TPC-H SF=10 | Large | ~10 GB |
| TPC-DS SF=1 | Default | ~1 GB |
| TPC-DS SF=10 | Large | ~10 GB |
| SSB SF=1 | Default | ~600 MB |
| SSB SF=10 | Large | ~6 GB |
| NYC Taxi 1 month | Subset | ~2 GB |
| NYC Taxi 6 months | Medium | ~12 GB |
| Stack Overflow Mini | 2008-2009 | ~1.5 GB |
| Stack Overflow 2010 | 2008-2010 | ~10 GB |
| ClickBench | Full | ~15 GB (compressed) |
| JOB/IMDB | Full | ~3.7 GB |
| Airline 10 years | Historical | ~5 GB |

### Recommended Starting Points

1. **Quick CI/Development Testing:**
   - TPC-H SF=1 (~1 GB)
   - SSB SF=1 (~600 MB)
   - JOB/IMDB (~3.7 GB)

2. **OLAP Benchmarking:**
   - TPC-H SF=10 (~10 GB)
   - ClickBench (~15 GB compressed)
   - NYC Taxi 1 year (~25 GB)

3. **Real-World Query Patterns:**
   - JOB - Complex joins, optimizer stress
   - Stack Overflow - Mixed workloads
   - ClickBench - Web analytics queries

4. **Time-Series Analysis:**
   - NYC Taxi - Temporal patterns
   - Airline - Seasonal trends
   - IoT/TSBS - High-frequency data

---

## Quick Start Commands

```bash
# TPC-H with DuckDB (easiest)
duckdb -c "
  INSTALL tpch;
  LOAD tpch;
  CALL dbgen(sf = 1);
  EXPORT DATABASE 'tpch_sf1' (FORMAT PARQUET);
"

# NYC Taxi (single month)
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# ClickBench
curl -O https://datasets.clickhouse.com/hits_compatible/hits.parquet

# JOB/IMDB
curl -OL https://bonsai.cedardb.com/job/imdb.tgz && tar xzf imdb.tgz
```

---

## References

- [TPC Official Website](https://www.tpc.org/)
- [Mark Litwintschik's Benchmark Summaries](https://tech.marksblogg.com/benchmarks.html)
- [ClickHouse Example Datasets](https://clickhouse.com/docs/getting-started/example-datasets)
- [DuckDB Extensions](https://duckdb.org/docs/stable/core_extensions/overview)
