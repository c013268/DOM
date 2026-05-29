# Runbooks — DOM Platform

## Common Scenarios

### Pipeline is paused after failure
1. Check Airflow UI for the failed task
2. Review task logs for root cause
3. Fix the underlying issue
4. Clear the failed task instance
5. Unpause the DAG

### Streaming job stuck / no new data
1. Check Databricks cluster status
2. Verify Kafka topic has new messages
3. Check checkpoint location for corruption
4. Restart the streaming job via Airflow

### dbt model failure
1. Check dbt Cloud run logs for SQL error
2. Verify source data exists in Stage/Bronze
3. Check for schema drift in upstream tables
4. Re-run the failed dbt job from Airflow

### Power BI refresh failed
1. Check if Gold layer data is fresh
2. Verify Power BI service credentials are valid
3. Check time-window filtering (some datasets only refresh in specific hours)
4. Manual refresh via Power BI Service portal

### ProductMasterSensor timeout
1. Check if `process_na_product_master_daily` DAG is stuck
2. Verify `na_product_master_build_gen2` task state
3. If product master is genuinely stuck, contact upstream team
4. Emergency: skip sensor by marking it success manually

### Data quality failures in post_gold_dq
1. Check `load_dom_dq_result_t` for failed rules
2. Identify affected entity and rule type
3. Investigate source data for root cause
4. Fix upstream or adjust DQ rule threshold
