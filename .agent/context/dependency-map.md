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

### `dbx_dom_global_daily` (runs first — data must land before dbt transforms)
- **Depends on:** Kafka topic availability, Databricks cluster pool
- **Produces:** Landing + Refined Delta tables in ADLS
- **Gated by:** `ProductMasterSensor` (waits for product master to finish)

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
