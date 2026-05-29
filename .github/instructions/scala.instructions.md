# Scala Instructions

## Code Style
- Follow Scala 2.12 conventions
- Use `case class` for data models
- Use pattern matching over if/else chains
- Prefer immutable collections

## Streaming Patterns
- Extend `AbstractApp` (or `AbstractApp_Gen2`) for new apps
- Use `foreachBatch` for micro-batch processing
- Handle late-arriving data with watermarks where applicable
- Always set checkpoint locations for fault tolerance

## Parallel Writes
- Use `scala.concurrent.Future` for writing to multiple destinations
- Use `Await.result` with proper timeout
- Wrap in try/catch for graceful error handling

## Delta Operations
- MERGE for upserts (use `src/main/resources/sqls/` for SQL templates)
- Include `WHEN MATCHED` and `WHEN NOT MATCHED` clauses
- Use `last_modified_ts` for change detection

## Schema
- Define JSON schemas in `src/main/resources/schema/`
- Use `StructType` for programmatic schema definition when needed
- Validate schema on read to catch upstream changes early

## Configuration
- Use `application.conf` for static config
- Runtime parameters passed as ordered args from Airflow
- Never hardcode connection strings — use Databricks secrets/scopes
