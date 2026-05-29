# Schema Registry — DOM Platform

## Databricks Delta Tables

### Landing Tables (`sales_landing_npii`)
| Table | Key Columns | Grain | Written By |
|-------|-------------|-------|------------|
| `oms_orders` | order_id, order_line_id, status | Order status event | Both `_Gen2` and `_DOM` |
| `oms_consignments` | consignment_id | Consignment event | Both `_Gen2` and `_DOM` |
| `oms_returns` | return_id | Return event | Both `_Gen2` and `_DOM` |
| `oms_refunds` | refund_id | Refund event | Both `_Gen2` and `_DOM` |
| `oms_exchanges` | exchange_id | Exchange event | `_DOM` only |

### Refined Tables (`sales_npii`)
| Table | Key Columns | Grain | Written By |
|-------|-------------|-------|------------|
| `oms_orders` | order_id, order_line_id | Latest order status | Both `_Gen2` and `_DOM` |
| `oms_consignments` | consignment_id | Latest consignment | Both `_Gen2` and `_DOM` |
| `oms_returns` | return_id | Latest return | Both `_Gen2` and `_DOM` |
| `oms_refunds` | refund_id | Latest refund | Both `_Gen2` and `_DOM` |
| `oms_parsed_tender` | tender_id | Parsed tender data | `_DOM` only |

### OBF Tables (`fulfillment_npii`)
| Table | Key Columns | Grain | Written By |
|-------|-------------|-------|------------|
| `obf_order_status_history` | order_id, status_ts | Status history | `_DOM` |

### Audit Table (`etl_stats_npii`)
| Table | Key Columns | Purpose | Used By |
|-------|-------------|---------|---------|
| `dom_oms_etl_audit` | job_name, run_id | Watermark tracking, execution audit | `_DOM` jobs only |

## Job Architecture — SQL File Patterns

### DOM SQL Files (`src/main/resources/sqls/DOM-*.sql`)
Multi-statement SQL files with this structure:
```
1. CREATE OR REPLACE TEMP VIEW source_view AS (SELECT ... FROM snowflake_stage)
2. CREATE OR REPLACE TEMP VIEW transform_view AS (SELECT ... business logic ...)
3. ... additional transform views (registered in parallel via Future) ...
4. MERGE INTO target_table USING final_view ON match_keys
     WHEN MATCHED THEN UPDATE SET ...
     WHEN NOT MATCHED THEN INSERT (...)
```

### Legacy OMS SQL Files (`merge*.sql`, `order*.sql`)
Simple single-statement merge/transform SQL:
```
MERGE INTO target USING source ON keys
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

## Snowflake — Gold Layer (Star Schema)

### Dimensions
| Table | Business Key | Type |
|-------|-------------|------|
| `dim_mao_cust_t` | customer_id | SCD Type 2 |
| `dim_mao_item_t` | item_id/sku | SCD Type 2 |
| `dim_mao_loc_t` | location_id | SCD Type 2 |
| `dim_mao_org_t` | org_id | SCD Type 2 |
| `dim_mao_employee_t` | employee_id | SCD Type 2 |
| `dim_mao_promo_t` | promo_id | SCD Type 2 |

### Facts
| Table | Grain | Key Dimensions |
|-------|-------|----------------|
| `fct_mao_ord_hdr_t` | Order header | cust, org, loc |
| `fct_mao_ord_line_t` | Order line | item, loc, promo |
| `fct_mao_ord_alloc_t` | Allocation | order, loc |
| `fct_mao_ord_pymt_line_t` | Payment line | order |
| `fct_mao_fulfillment_hdr_t` | Fulfillment header | order, loc |
| `fct_mao_fulfillment_line_t` | Fulfillment line | order, item |
| `fct_mao_fulfillment_package_detail_t` | Package | fulfillment |
| `fct_mao_inv_supply_t` | Inventory supply | item, loc |
| `fct_mao_ord_hdr_hist_t` | Order header history | order |
| `fct_mao_ord_fulfillment_dtl_t` | Order fulfillment detail | order |
| `fct_mao_sku_exclusion_t` | SKU exclusion rules | item |
| `fct_mao_inv_location_atp_t` | Location ATP | item, loc |
| `fct_mao_inv_network_atp_t` | Network ATP | item |

## JSON Schemas (Spark Ingestion)
Located in `src/main/resources/schema/`:
- `orders.json` — OMS order event structure
- `consignments.json` — Consignment event structure
- `returns.json` — Return event structure
- `refunds.json` — Refund event structure
- `exchanges.json` — Exchange event structure
- `oms0406.json` — Legacy OMS format (order header/line combined)
