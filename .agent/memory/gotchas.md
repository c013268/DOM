# Gotchas & Edge Cases

Non-obvious behaviors, tricky patterns, and things that can trip you up.

---

## Scala / Databricks

### `_Gen2` vs `_DOM` — Same Base Class, Different Architectures
- Both `_Gen2` and `_DOM` classes extend `AbstractApp_Gen2` — don't assume they work the same way
- `_Gen2` = Kafka streaming with `foreachBatch`; `_DOM` = batch SQL execution with MERGE
- The `_Gen2` in `AbstractApp_Gen2` is misleading — it's a shared base class for both

### DOM SQL Files are Multi-Statement
- SQL files in `sqls/DOM-*.sql` contain MULTIPLE statements separated by semicolons
- They're split and executed sequentially/in-parallel — NOT sent as one query
- Substitution variables (`${var}`) are replaced BEFORE execution via `processSubstitutions`

### Weekly VACUUM/OPTIMIZE Only on Sundays
- `weeklyVacuumAndOptimize()` checks `DayOfWeek == 7` (Sunday)
- Also checks DESCRIBE HISTORY to avoid duplicate runs on same day
- RETAIN 168 HOURS (7 days) for VACUUM

### Audit Watermark Determines Load Window
- `INCREMENTAL` uses `getEffectiveWatermark` → last successful `last_processed_ts`
- `FULL` uses hardcoded `1900-01-01 00:00:00` (loads everything)
- Both refined and landing have SEPARATE watermarks — the earlier one is used for merge predicates

### `run_table_group` vs `run_table_name`
- `run_table_group` is comma-separated list (e.g., `"exchanges,tenders"`) — runs in sequence
- `run_table_name` is single table or `"all"` — fallback when `run_table_group` is empty
- Empty string for either defaults to `"all"` (runs everything)

### DOM Merge Target Predicate Buffer
- `mergeTargetBufferDays = 100` is added ON TOP of `source_lookback_days`
- This ensures the MERGE target scan covers enough historical data
- Without it, late-arriving updates to old orders might be missed

## Airflow

### Config is in `dom_prod_json` Variable — NOT in DAG file
- All job IDs, table names, cluster configs live in the JSON variable
- Editing the DAG file alone won't change runtime behavior

### `dom_table_groups` Controls Parallel Execution
- Tables in the SAME inner array run in the same Databricks job (parallel within job)
- Tables in DIFFERENT inner arrays run as separate Databricks jobs (parallel across jobs)
- Example: `[["exchanges","tenders"]]` = one job running both tables

### Non-Priority Branch Skips on Manual Triggers
- `BranchPythonOperator` checks `run_type != "scheduled"` → skips non-priority
- Manual DAG triggers will ALWAYS skip non-priority tasks

## dbt

### Priority vs Non-Priority — Different dbt Cloud Job IDs
- Priority: `dom_dbt_job_id_journal_to_bronze` / `dom_dbt_job_id_stage_to_silver`
- Non-priority: `dom_dbt_journal_to_bronze_nonpriority` / `dom_dbt_stage_to_silver_nonpriority`
- These are SEPARATE dbt Cloud jobs with different model selectors
