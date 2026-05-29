PROJECT CONTEXT & SETUP INSTRUCTIONS

CURRENT STATE:
- All agent-related infrastructure files are placeholder files with no actual content
- Need to remove unnecessary agent-related markdown files
- Goal: Create a proper agentic framework with dedicated agents for different tasks

PROJECT OVERVIEW (Backtracked from BI Reports to Source):

DATA ARCHITECTURE:
1. Layer Structure (5 layers):
   - Stage → Bronze (backup only) → Silver (starting point) → Gold → Platinum
   - Silver to Platinum: All transformations done via DBT on Snowflake
   - Silver to Platinum: All tables are Iceberg tables

2. Target Tables (8 main tables in Snowflake):
   - OrderLine, OrderHeader, Returns, Refunds, Exchanges, Consignments, 
     OBF_Order_Status_History, Tenders
   - Note: In Databricks, OrderLine and OrderHeader are combined into a single "Orders" table

3. Target Systems (3 destinations):
   a) Snowflake - Used for all dashboard reporting
   b) Databricks Landing (Delta tables) - Contains all order statuses (grain: order status)
   c) Databricks Refined (Delta tables) - Contains latest order status (grain: order ID)

JOB ARCHITECTURE:
- Two separate job types (visible in Airflow):
  1. DBT Job - Complex framework with multiple models (requires deep dive to understand)
  2. Legacy Streaming + Current Databricks Scala Job
     - Dynamic jobs with dedicated clusters per job
     - Custom resource allocation per job
     - Parallel writing jobs implemented using Scala FUTURE module

BUSINESS CONTEXT:
- Migration project from Legacy OMS system to MAO system
- Involves table refactoring and implementing new business logic

IMMEDIATE TASK:
1. First, analyze and understand the complete project structure
2. Create dedicated agents to handle specific tasks for future work
3. Build out a proper agentic framework
4. Clean up unnecessary agent-related files

Please analyze the codebase structure and propose an agent framework architecture.