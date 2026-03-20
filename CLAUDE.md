# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Spark — a unified analytics engine for large-scale data processing. Version 4.2.0-SNAPSHOT, Scala 2.13, Java 17+.

## Build Commands

Prefer SBT over Maven for day-to-day development (faster incremental compilation). Maven is the build of reference for packaging.

```bash
# SBT build (preferred for development)
./build/sbt package

# Maven build
./build/mvn -DskipTests clean package

# With Hive support (needed for PySpark tests)
./build/sbt -Phive clean package

# With YARN support
./build/mvn -Pyarn -Dhadoop.version=3.4.3 -DskipTests clean package

# With Kubernetes support
./build/mvn -Pkubernetes -DskipTests clean package
```

SBT interactive mode (`./build/sbt` with no args) avoids repeated JVM startup overhead.

## Running Tests

```bash
# All tests (SBT)
./build/sbt test

# All tests (Maven)
./build/mvn test

# Single test class (SBT) — use the subproject prefix
./build/sbt "core/testOnly org.apache.spark.rdd.RDDSuite"
./build/sbt "sql/testOnly org.apache.spark.sql.SQLQuerySuite"

# Full test suite via dev script
./dev/run-tests

# PySpark tests (build with Hive first)
./build/sbt -Phive clean package
./build/sbt test:compile
./python/run-tests

# Individual PySpark test
./python/run-tests --testnames pyspark.sql.tests.test_arrow

# PySpark tests for a module
./python/run-tests --python-executables=python3 --modules=pyspark-sql

# Docker integration tests
./build/sbt -Pdocker-integration-tests docker-integration-tests/test
```

On macOS, set `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` when running PySpark tests.

## Linting and Style

```bash
./dev/lint-java        # Java checkstyle
./dev/lint-scala       # Scala scalastyle
./dev/lint-python      # Python (ruff/black/mypy)
```

Scala formatting: `dev/.scalafmt.conf` — maxColumn=98, Scala 2.13 dialect.

## Architecture

### Core Modules
- `core/` — Spark Core: RDDs, scheduling, shuffle, storage, networking
- `sql/catalyst/` — Query plan representation, analysis, optimization rules, code generation
- `sql/core/` — Spark SQL engine, DataFrames, data sources, physical planning
- `sql/api/` — Public SQL API surface
- `sql/hive/` — Hive metastore integration
- `sql/hive-thriftserver/` — JDBC/ODBC Thrift server
- `sql/connect/` — Spark Connect (client-server remote execution protocol)
- `sql/pipelines/` — Declarative SQL pipelines

### Common Libraries
- `common/unsafe/` — Off-heap memory management, row format
- `common/network-common/` — RPC framework and transport
- `common/network-shuffle/` — External shuffle service
- `common/sketch/` — Probabilistic data structures (CountMinSketch, etc.)
- `common/variant/` — Semi-structured Variant data type

### Other Modules
- `mllib/` — Distributed machine learning (DataFrame-based API)
- `streaming/` — DStream-based streaming (legacy; prefer Structured Streaming in sql/core)
- `graphx/` — Graph computation
- `resource-managers/kubernetes/` — K8s scheduler backend
- `resource-managers/yarn/` — YARN scheduler backend
- `connector/` — Data source connectors (Avro, Kafka, Protobuf)
- `python/` — PySpark implementation
- `repl/` — Spark shell (Scala REPL)

### Key Patterns
- Query planning pipeline: parsing → analysis (`sql/catalyst/analysis/`) → optimization (`sql/catalyst/optimizer/`) → physical planning (`sql/core/execution/`) → code generation
- Catalyst rules extend `Rule[LogicalPlan]` or `Rule[SparkPlan]` and are composed into batches
- Data sources implement the V2 API in `sql/catalyst/connector/`
- Configuration is managed via `SQLConf` (SQL) and `SparkConf` (core)

## SBT Memory

Configured in `.sbtopts`: `-Xmx8g -Xms4g -XX:MaxMetaspaceSize=1g`

For Maven, set `MAVEN_OPTS="-Xss64m -Xmx4g -Xms4g -XX:ReservedCodeCacheSize=128m"` (auto-set by `build/mvn`).

## Protobuf

Spark Connect and streaming use protobuf. Regenerate with:
```bash
./dev/gen-protos.sh              # Core protos
./dev/connect-gen-protos.sh      # Spark Connect protos
./dev/streaming-gen-protos.sh    # Streaming protos
```
