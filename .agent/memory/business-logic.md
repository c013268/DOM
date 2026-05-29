# Business Logic

User-defined business rules, data transformations, and domain-specific logic.

---

## Order Lifecycle
- Orders flow through statuses: Created → Allocated → Shipped → Delivered (or Cancelled)
- `orderStatus` field drives the state machine
- Cancelled orders have `cancelCode` and `cancelReason` populated

## Entity Relationships
- **OrderHeader** 1:N **OrderLine** (split in Snowflake, combined in Databricks as "orders")
- **Order** 1:N **Consignments** (fulfillment shipments)
- **Order** 1:N **Returns** (customer returns)
- **Return** 1:1 **Refund** (financial refund for a return)
- **Order** 1:N **Exchanges** (exchange transactions)
- **Order** 1:N **Tenders** (payment instruments used)

## Load Type Logic
- `INCREMENTAL` — Only process records newer than last successful watermark
- `FULL` — Reprocess all records from beginning (uses `1900-01-01` as watermark)
- Per-table override via `tableLoadTypes` JSON in config
- Global default from `loadType` parameter

## Merge/Upsert Logic (DOM Jobs)
- Match on business keys (order_id + line_id, consignment_id, etc.)
- WHEN MATCHED: Update all columns (full row replace)
- WHEN NOT MATCHED: Insert new row
- Target predicate uses date-bounded scan for performance (avoids full table scan on MERGE)

## Weekly Maintenance
- VACUUM: Removes old Delta files (7-day retention = 168 hours)
- OPTIMIZE: Compacts small files for better read performance
- Runs only on Sundays to avoid impacting weekday pipeline performance

## Priority vs Non-Priority
- **Priority entities:** Orders, Consignments — need near-real-time updates for operational dashboards
- **Non-priority entities:** Exchanges, historical lookups — can be delayed to once daily
- Non-priority hour: 18 (6 PM EST) — configured in `dom_non_priority_hour`

---

*(Add new business logic rules as they are discovered or communicated by stakeholders)*
