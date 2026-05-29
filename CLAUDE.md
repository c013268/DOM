# DOM Project — AI Agent Context

## Project Identity
**Name:** DOM (Distributed Order Management) Data Platform  
**Owner:** DAE (Data & Analytics Engineering) Team, Foot Locker  
**Purpose:** End-to-end data pipeline for the complete order lifecycle — from event ingestion to BI-ready dimensional models.

## Architecture Overview

### Data Flow
```
Confluent Kafka (OMS/OBF Events)
    ↓
Databricks Spark Streaming (Scala JAR)
    ↓
Azure Data Lake Storage Gen2 (Delta/Landing)
    ↓
Snowflake (Stage → Bronze → Silver → Gold → Platinum)
    ↓  All layers use Iceberg tables (Silver onward)
Power BI Dashboards
```

### Layer Definitions
| Layer | Purpose | Storage |
|-------|---------|---------|
| Stage | Raw event landing from Kafka via Spark | ADLS Gen2 Delta |
| Bronze | Journal/backup copy from Stage | Snowflake Iceberg |
| Silver | Cleaned, conformed, historized (starting point for analytics) | Snowflake Iceberg |
| Gold | Star schema dimensional model (MAO mart) | Snowflake Iceberg |
| Platinum | Aggregated/reporting views | Snowflake Iceberg |

### Target Tables (8 core entities)
1. **Orders** (OrderHeader + OrderLine — split in Snowflake, combined in Databricks)
2. **Consignments** — shipment fulfillment records
3. **Returns** — customer returns
4. **Refunds** — financial refunds
5. **Exchanges** — order exchanges
6. **Tenders** — payment instruments
7. **OBF_Order_Status_History** — order status lifecycle events
8. *(OrderHeader and OrderLine are separate in Snowflake)*

### Target Systems
| System | Purpose | Grain |
|--------|---------|-------|
| Snowflake | All dashboard reporting (dbt transformations) | Varies by table |
| Databricks Landing (Delta) | All order statuses | Order Status |
| Databricks Refined (Delta) | Latest order status only | Order ID |

## Repository Structure

### `DAE-databricks-ingestion-streaming-scala-oms-master/`
- **Language:** Scala 2.12 on Spark (Databricks)
- **Purpose:** Real-time Kafka → ADLS Delta ingestion
- **Entry points:** `AdlsLoadMain_DOM` (OMS), `OBF_Order_Status_History_DOM` (OBF)
- **Key patterns:** Structured Streaming, Future-based parallel writes, merge upserts

### `DAE-DBT-DOM-PROJECT-master/`
- **Language:** SQL (dbt on Snowflake)
- **Purpose:** Medallion architecture transformations (Stage → Bronze → Silver → Gold)
- **Key patterns:** Priority/Non-priority load tiers, Framework models, DQ rules engine
- **Domains:** ord_mgmt, ord_fulflmnt, inv_mgmt, itm, org, payment

### `DAE-DOM-Airflow-Dags/`
- **Language:** Python (Apache Airflow)
- **Purpose:** Orchestration of both Databricks and dbt Cloud jobs
- **DAGs:** `dbt_dom_global_daily` (dbt pipeline), `dbx_dom_global_daily` (Databricks streaming)
- **Schedule:** Every 15 minutes (`*/15 * * * *`)
- **Key patterns:** ProductMasterSensor gate, priority/non-priority branching, Power BI refresh

## Business Context
- **Migration:** Legacy OMS → MAO (Multi-Attribute Order) system
- **Involves:** Table refactoring, new business logic, schema evolution
- **Brands:** Foot Locker NA (North America)

## Coding Conventions
- **Scala:** Follow existing patterns in `com.footlocker.oms_streaming.apps`
- **dbt:** Medallion layers, priority/nonpriority framework, `mao_` prefix for Silver, `dim_`/`fct_` for Gold
- **Airflow:** Use `DbtCloudRunJobOperator`, `DatabricksSubmitRunOperator`, config from `dom_prod_json`
- **SQL:** Snowflake dialect, Iceberg table format

## Job Architecture
- **dbt Jobs:** Triggered via dbt Cloud API through Airflow operators
  - Batch Start → Journal to Bronze → Stage to Silver → Bronze to Silver Hist → Silver to Gold → Gold to Refactored → Post-Gold DQ → Batch End
- **Databricks Jobs:** Dynamic clusters per job, custom resource allocation
  - OMS Streaming (orders, consignments, returns, refunds, exchanges, tenders)
  - OBF Streaming (order status history)
  - Scala FUTURE module for parallel writes
