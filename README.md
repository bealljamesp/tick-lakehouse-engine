# Market Tick Ingestion & Lakehouse Engine (`tick-lakehouse-engine`)

A high-throughput streaming and analytical query pipeline for market tick ingestion. Demonstrates an end-to-end lakehouse architecture: ingestion of raw financial ticks/trades, zero-copy columnar batch transformations via Polars and Arrow, partitioned Parquet storage in MinIO/S3, and sub-second analytical aggregations via embedded DuckDB.

---

## 1. Architectural & Engineering Directives

- **Runtime & Typing:** Python 3.12+; PEP 585/604 generics and strict typed models (`Pydantic v2` for ingestion boundaries, PyArrow schemas for storage).
- **Streaming & Backpressure:** Asynchronous producer-consumer via Redpanda/Kafka (`aiokafka`) simulating real-world order book trades and quote bursts.
- **Zero-Copy Transformation:** Polars streaming/lazy execution engine (`polars.LazyFrame`) transforming raw event batches into optimized columnar buffers without intermediate Python object overhead.
- **Storage Layer (Lakehouse Pattern):** Append-only, partitioned Apache Parquet files stored in S3/MinIO using standard analytical Hive partitions: `data/ticks/date=YYYY-MM-DD/symbol=TICKER/*.parquet`.
- **Query & Analytics Core:** In-process DuckDB analytical SQL layer executing zero-copy scans across remote/local Parquet partitions for 1-second VWAP, rolling spreads, and depth aggregations.

---

## 2. Directory Layout

```text
tick-lakehouse-engine/
├── pyproject.toml
├── docker-compose.yml       # Redpanda, MinIO (S3-compatible), and local services
├── README.md
├── src/
│   └── lakehouse/
│       ├── __init__.py
│       ├── schemas.py       # Pydantic ingest schemas & PyArrow static tables
│       ├── producer.py      # Async tick stream generator pushing to Redpanda
│       ├── consumer.py      # Micro-batch consumer writing columnar Parquet
│       └── analytics.py     # DuckDB query interfaces for VWAP & OHLCV rollup
└── tests/
    ├── test_ingest_schema.py
    └── test_duckdb_queries.py
```
