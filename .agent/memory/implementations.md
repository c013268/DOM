# Implementations Log

Record of new implementations, code changes, and features added to the project.

---

## Format
```
### [YYYY-MM-DD] Title
- **Component:** (Scala/dbt/Airflow)
- **Files Modified:** list of files
- **Description:** what was done and why
- **Notes:** any follow-up considerations
```

---

### [2026-05-29] Agent Framework Setup
- **Component:** Infrastructure
- **Files Modified:** All `.github/agents/`, `.github/instructions/`, `.agent/context/`, CLAUDE.md, AGENTS.md
- **Description:** Created comprehensive agent framework with 4 specialized agents (dbt, scala, airflow, review), instruction files, and shared context documentation.
- **Notes:** Scala agent updated to distinguish `_Gen2` (Legacy OMS streaming) from `_DOM` (MAO batch) job types.
