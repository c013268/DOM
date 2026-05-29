# Dependency Map — DOM Platform

## System Dependencies

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Confluent Kafka │────▶│   Databricks    │────▶│   Snowflake     │
│ (Source Events) │     │ (Spark Stream)  │     │ (dbt Transform) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                              ┌───────────────────────────┘
                              ▼
                        ┌─────────────┐
                        │  Power BI   │
                        │ (Dashboards)│
                        └─────────────┘
```

## DAG Dependencies

### `dbx_dom_global_daily` — Databricks Jobs (runs first)
Contains TWO sub-architectures:

**A) DOM Jobs (`_DOM` suffix) — Active/Primary:**
- **Trigger:** Airflow `DatabricksSubmitRunOperator` every 15 min
- **Entry Point:** `AdlsLoadMain_DOM` (main class)
- **Mechanism:** Reads from Snowflake Stage → SQL transforms → Delta MERGE
- **Produces:** Landing + Refined Delta tables
- **Config:** `dom_scala` section + `dom_table_groups` for parallel execution
- **Audit:** Writes to `prod.etl_stats_npii.dom_oms_etl_audit`
- **Gated by:** `ProductMasterSensor`

**B) Legacy OMS Streaming (`_Gen2` suffix) — Maintenance Mode:**
- **Trigger:** Airflow `DatabricksSubmitRunOperator` (streaming mode)
- **Entry Point:** `AdlsLoadMain_Gen2` (main class)
- **Mechanism:** Kafka Structured Streaming → parse messageType → write to ADLS + Snowflake
- **Produces:** Raw string landing + entity-specific Delta tables
- **Config:** `dom_oms_scala` section + `dom_oms_params` ordered args
- **Source:** Confluent Kafka topic

### `dbt_dom_global_daily` (runs after streaming lands data)
- **Depends on:** Fresh data in Snowflake Stage (from Databricks writes)
- **Produces:** Bronze → Silver → Gold tables, DQ results
- **Triggers:** Power BI dataset refreshes post-Gold

## Cross-DAG Dependencies
- `process_na_product_master_daily` → `dbx_dom_global_daily` (via ProductMasterSensor)
- `dbx_dom_global_daily` → `dbt_dom_global_daily` (implicit — data freshness)

## dbt Model Dependencies (simplified)
```
batch_start
    ├── dom_priority_*_bronze (journal to bronze)
    │       └── dom_priority_*_silver (stage to silver)
    │               └── mao_*_t (silver hist models)
    │                       └── dim_mao_* / fct_mao_* (gold)
    │                               └── refactored models
    │                                       └── post_gold_dq
    └── dom_nonpriority_*_bronze (daily only)
            └── dom_nonpriority_*_silver
batch_end
```

## External Dependencies
| System | Dependency | Impact if Unavailable |
|--------|-----------|----------------------|
| Confluent Kafka | Source events | No new data ingested |
| Azure ADLS Gen2 | Storage layer | Streaming jobs fail |
| Databricks | Compute | No ingestion or Delta ops |
| Snowflake | Warehouse | No dbt transformations |
| dbt Cloud | Orchestration API | dbt jobs can't be triggered |
| Power BI Service | Refresh API | Dashboards show stale data |
| Product Master DAG | Upstream data | DOM DAG waits (sensor) |
