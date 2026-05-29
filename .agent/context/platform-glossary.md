# Platform Glossary — DOM

| Term | Definition |
|------|-----------|
| **DOM** | Distributed Order Management — the overall platform |
| **MAO** | Manhatten Active OMNI — new order management system replacing Legacy OMS |
| **OMS** | Order Management System — legacy system being migrated from |
| **OBF** | Order Broker Fulfillment — fulfillment status tracking system |
| **ATP** | Available-to-Promise — inventory availability for order promising |
| **DAE** | Data & Analytics Engineering — the team owning this platform |
| **Medallion** | Architecture pattern: Stage → Bronze → Silver → Gold → Platinum |
| **Priority** | Time-sensitive loads that run every 15-min cycle |
| **Non-Priority** | Less urgent loads that run once daily (hour 18 EST) |
| **Framework** | The numbered-step pattern for Bronze/Silver models (e.g., `dom_priority_1_ord_mgmt_bronze`) |
| **Refactored** | Intermediate models bridging Legacy OMS → MAO logic |
| **Landing Table** | Databricks Delta table with ALL historical statuses (merge on status key) |
| **Refined Table** | Databricks Delta table with LATEST status only (upsert grain) |
| **DQ** | Data Quality — rules-based validation framework in Silver layer |
| **Iceberg** | Open table format used for all Snowflake tables (Silver onward) |
| **Product Master** | Upstream DAG that builds item/product dimension; DOM waits for it |
| **NPII** | Non-Personally Identifiable Information — data classification tier |
| **PII** | Personally Identifiable Information — sensitive data tier |
| **dbt Cloud** | SaaS platform running dbt transformations, triggered via API |
| **dom_prod_json** | Central Airflow Variable (JSON) containing all pipeline configuration |
| **Batch ID** | Tracking identifier for each pipeline execution cycle |
| **varSubstitutions** | dbt variable overrides passed via dbt Cloud job triggers |
