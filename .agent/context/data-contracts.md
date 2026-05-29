# Data Contracts — DOM Platform

## Kafka Topics → Databricks Landing

| Kafka Topic | Target Landing Table | Schema |
|-------------|---------------------|--------|
| `com_footlocker_dap_pii_oms_order_events` | `{catalog}.sales_landing_npii.oms_orders` | `schema/orders.json` |
| `com_footlocker_dap_pii_oms_order_events` | `{catalog}.sales_landing_npii.oms_consignments` | `schema/consignments.json` |
| `com_footlocker_dap_pii_oms_order_events` | `{catalog}.sales_landing_npii.oms_returns` | `schema/returns.json` |
| `com_footlocker_dap_pii_oms_order_events` | `{catalog}.sales_landing_npii.oms_refunds` | `schema/refunds.json` |
| `com_footlocker_dap_pii_oms_order_events` | `{catalog}.sales_landing_npii.oms_exchanges` | `schema/exchanges.json` |
| `com_footlocker_cr_obf_fl_na_fulfillment_order_status_updates` | `{catalog}.fulfillment_npii.obf_order_status_history` | N/A |

## Databricks → Snowflake (Stage)

| Databricks Table | Snowflake Stage Table | Load Type |
|------------------|-----------------------|-----------|
| `oms_orders` | Stage orders (OMS0406 format) | INCREMENTAL |
| `oms_consignments` | Stage consignments | INCREMENTAL |
| `oms_returns` | Stage returns | INCREMENTAL |
| `oms_refunds` | Stage refunds | INCREMENTAL |
| `oms_exchanges` | Stage exchanges | INCREMENTAL |
| `oms_parsed_tender` | Stage tenders | INCREMENTAL |
| `obf_order_status_history` | Stage OBF history | INCREMENTAL |

## Snowflake Layer Contracts

### Bronze → Silver
- 1:1 mapping with data type enforcement
- NULL handling rules applied
- Deduplication on primary keys
- Batch ID tracking added

### Silver → Gold
- Star schema denormalization
- Business key resolution
- SCD Type 2 for historical dimensions
- Aggregation where applicable

## SLAs
- Pipeline frequency: Every 15 minutes
- End-to-end latency target: < 20 minutes (Kafka event → Gold table)
- Non-priority entities: Once daily at 18:00 EST
