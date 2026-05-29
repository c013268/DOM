# Airflow/Python Instructions

## DAG Style
- Use context manager (`with DAG(...) as dag:`) syntax
- Set `catchup=False` and `max_active_runs=1`
- Always include `PlatformNotifyOperator` with `trigger_rule=TriggerRule.ONE_FAILED`
- Use `pendulum` for timezone-aware datetimes

## Configuration
- All runtime config from `dom_prod_json` Airflow Variable
- Never hardcode job IDs, table names, or cluster configs
- Use `Variable.get()` with `deserialize_json=True`

## Operators
- `DbtCloudRunJobOperator` — for dbt Cloud jobs
- `DatabricksSubmitRunOperator` — for Scala JAR jobs
- `BranchPythonOperator` — for priority/non-priority routing
- `PythonOperator` — for utility tasks (timestamps, Power BI refresh)

## Error Handling
- Use `failtask()` pattern to pause DAG on failure (prevent deadlock)
- Set appropriate `retries` and `retry_delay`
- Log context (execution_date, run_type) in branching decisions

## Sensors
- `ProductMasterSensor` — custom gate for upstream dependencies
- Always use `mode='reschedule'` (release worker slots)
- Set reasonable `timeout` and `poke_interval`
