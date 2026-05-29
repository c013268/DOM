# SQL Instructions

## Dialect
- **Snowflake SQL** for dbt models
- **Delta Lake SQL** for Databricks merge scripts

## Style
- Use UPPERCASE for SQL keywords (`SELECT`, `FROM`, `WHERE`, `JOIN`)
- Use lowercase for column names and aliases
- One column per line in SELECT statements
- Indent JOIN/WHERE/GROUP BY clauses
- Use explicit `INNER JOIN` / `LEFT JOIN` (never implicit joins)

## CTEs
- Use CTEs (`WITH`) over subqueries
- Name CTEs descriptively
- Final CTE named `final`

## Snowflake Specifics
- Use `QUALIFY` for window function filtering
- Use `TRY_CAST` for safe type conversions
- Use `COALESCE` for NULL handling
- Use `IFF` for simple conditionals (vs CASE for complex)

## Delta Merge (Databricks)
```sql
MERGE INTO target USING source
  ON target.pk = source.pk
WHEN MATCHED AND source.last_modified_ts > target.last_modified_ts THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...)
```
