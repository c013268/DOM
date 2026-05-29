# dbt Instructions

## Model Structure
- Use `{{ config(...) }}` block at the top of every model
- Use CTEs for readability — no nested subqueries
- Final CTE should be named `final` and selected at the end
- Always include `dbt_batch_id` column for audit tracking

## Materialization
- **Bronze**: `incremental` with `unique_key` and `merge_update_columns`
- **Silver**: `incremental` with `unique_key`, Iceberg format
- **Gold**: `incremental` or `table` depending on size, Iceberg format
- Use `on_schema_change='sync_all_columns'` for evolving sources

## Naming
- Models: `{prefix}_{entity}_t.sql` (Gold: `dim_mao_*_t`, `fct_mao_*_t`)
- Sources: `{layer}_{domain}_source.yml`
- Schema: `{layer}_schema.yml`

## Testing
- Every model needs: `not_null` on PK, `unique` on PK
- Relationships tests for foreign keys
- Use `severity: warn` for non-blocking DQ issues

## Jinja/Macros
- Use `{{ ref('model_name') }}` for model dependencies
- Use `{{ source('source_name', 'table_name') }}` for raw sources
- Use `{{ var('variable_name') }}` for dbt variables (passed via `varSubstitutions`)
