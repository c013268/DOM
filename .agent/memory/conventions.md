# Conventions & Patterns

Project-specific conventions, coding patterns, and standards discovered during development.

---

## File Naming

### Scala Apps
| Suffix | Meaning | Status |
|--------|---------|--------|
| `_DOM` | New MAO system batch jobs | **Active** — all new work here |
| `_Gen2` | Legacy OMS streaming jobs | **Maintenance** — avoid changes unless necessary |
| No suffix | Original Gen1 code | **Deprecated** — do not use |

### SQL Files
| Prefix | Job Type | Pattern |
|--------|----------|---------|
| `DOM-` | DOM (MAO) | Multi-statement (views + MERGE) |
| `merge` | Legacy OMS | Single MERGE statement |
| `order` | Legacy OMS | Order-specific transforms |

### dbt Models
| Prefix | Layer | Example |
|--------|-------|---------|
| `dom_priority_*_bronze` | Bronze (priority) | `dom_priority_1_ord_mgmt_bronze` |
| `dom_nonpriority_*_bronze` | Bronze (non-priority) | `dom_nonpriority_1_ord_mgmt_bronze` |
| `mao_*_t` | Silver | `mao_ord_order_line_t` |
| `dim_mao_*_t` | Gold (dimension) | `dim_mao_cust_t` |
| `fct_mao_*_t` | Gold (fact) | `fct_mao_ord_hdr_t` |

## Code Patterns

### DOM Entity Handler Template
```scala
object Adls{Entity}_DOM extends Logging {
  private def extractViewName(stmt: String): String = { ... }
  private def registerViewsParallel(spark, stmts, tierLabel, ec): Unit = { ... }
  
  def dom_{entity}_load(spark: SparkSession, table: String, loadType: String, varSubstitutions: String): Unit = {
    val jobName = "DOM_OMS_{ENTITY}_LOAD"
    val auditTable = "prod.etl_stats_npii.dom_oms_etl_audit"
    val lastProcessedTs = getEffectiveWatermark(spark, auditTable, jobName, loadType)
    
    // Read SQL file, apply substitutions
    // Register views (parallel where possible)
    // Execute MERGE
    // Update audit on success/failure
  }
}
```

### Parallel View Registration
```scala
implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
val futures = stmts.map(stmt => Future { spark.sql(stmt) })
Await.result(Future.sequence(futures), 5.minutes)
```

### Variable Substitutions
```scala
val baseMap = stringToMap(varSubstitutions)  // "key1=val1;key2=val2" → Map
val processedSql = processSubstitutions(rawSql, substitutionMap)  // Replace ${var} tokens
```

## Configuration Conventions
- All runtime config lives in `dom_prod_json` Airflow Variable
- Table names always fully qualified: `{catalog}.{schema}.{table}`
- Parameters to Scala jobs are key=value Maps (DOM) or ordered arrays (Legacy OMS)
- Spark configs are set at the start of `execute()`, not in cluster config

## Git/PR Conventions
- Update `CHANGELOG.md` for every meaningful change
- One entity per PR when adding new DOM handlers
- Include both landing and refined table support unless entity only needs one
