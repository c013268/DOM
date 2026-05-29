# Python Instructions

## General
- Python 3.9+ compatible
- Use type hints for function signatures
- Use f-strings for string formatting
- Use `logging` module (not `print`)

## Imports
- Standard library → third-party → local (separated by blank lines)
- Avoid wildcard imports (`from x import *`)

## Airflow Specific
- Use Airflow's `@task` decorator or operator classes (not raw functions)
- Access context via `**context` parameter
- Use `Variable` and `Connection` classes for external config
