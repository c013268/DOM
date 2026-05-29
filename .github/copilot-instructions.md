# GitHub Copilot Instructions — DOM Project

## Project Overview
This is Foot Locker's Distributed Order Management (DOM) data platform. It processes the complete order lifecycle through a multi-layer architecture using Databricks (Scala streaming), Snowflake (dbt transformations), and Airflow (orchestration).

## Key Rules

### General
- All Snowflake tables from Silver onward are **Iceberg tables**
- The pipeline runs every **15 minutes** via Airflow
- Configuration is driven by the `dom_prod_json` Airflow variable
- This is a **migration project** from Legacy OMS to MAO — both patterns coexist

### When modifying dbt models:
- Follow the **Medallion Architecture**: Stage → Bronze → Silver → Gold
- Use the **Framework pattern** (priority/nonpriority) for Bronze and Silver
- Gold layer uses **star schema** naming: `dim_mao_*` for dimensions, `fct_mao_*` for facts
- Silver uses `mao_` prefix with domain: `mao_ord_*`, `mao_ful_*`
- Always add source YAML files alongside new models
- Respect the DQ framework in Silver for data quality checks

### When modifying Scala streaming jobs:
- Entry point is `AdlsLoadMain_DOM` for OMS entities
- Use `Future` module for parallel write operations
- Follow the existing `AdlsXxx_DOM.scala` naming pattern
- Schema definitions live in `src/main/resources/schema/*.json`
- Merge SQL lives in `src/main/resources/sqls/*.sql`

### When modifying Airflow DAGs:
- All config comes from `dom_prod_json` Variable (JSON)
- Use `DbtCloudRunJobOperator` for dbt jobs
- Use `DatabricksSubmitRunOperator` for Scala jobs
- Include `PlatformNotifyOperator` for failure notifications
- Use `ProductMasterSensor` as a gate when product master dependency exists
- Priority tasks run every cycle; non-priority runs once daily at configured hour

### Naming Conventions
| Component | Pattern | Example |
|-----------|---------|---------|
| dbt Gold dim | `dim_mao_{entity}_t` | `dim_mao_cust_t` |
| dbt Gold fact | `fct_mao_{entity}_t` | `fct_mao_ord_hdr_t` |
| dbt Silver | `mao_{domain}_{entity}_t` | `mao_ord_order_line_t` |
| dbt Bronze framework | `dom_{priority}_{n}_{domain}_bronze` | `dom_priority_1_ord_mgmt_bronze` |
| Scala app | `Adls{Entity}_DOM` | `AdlsOrders_DOM` |
| Databricks table (landing) | `{catalog}.sales_landing_npii.oms_{entity}` | `dev.sales_landing_npii.oms_orders` |
| Databricks table (refined) | `{catalog}.sales_npii.oms_{entity}` | `dev.sales_npii.oms_orders` |
