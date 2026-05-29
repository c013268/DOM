# Change log

All notable changes are documented in this file. Release numbers follow [Semantic Versioning](http://semver.org)
## [Unreleased]

## [4.17.0] - 2026-05-28
### Updated
- Consignments, oms orders exchnages rollout

## [4.16.0] - 2026-05-19
### Updated
- Consignments, Returns, Refunds, Exchanges Payment Info Fixes
- OBF & Consignments WH Location logic
- Consignments - Remove Rejections, OBF - Rejections
- Refunds - De-Dup on Return Orders
- Returns - XstoreTransationID column Fix
  
## [4.15.0] - 2026-05-14
### Updated
- ATP code to use UTC timestamps for audit
- Appeasements and returns change
- Amount columns to stay consistant through out the journey
  
## [4.14.0] - 2026-05-05
### Updated
- Tenders change for oms job

## [4.13.0] - 2026-05-05
### Updated
- Created Landing Merge Logics for DOM.

## [4.12.0] - 2026-05-05
### Updated
- New optimisation changes by decoupling dags.

## [4.11.0] - 2026-04-28
### Updated
- Tax and coupon changes.

## [4.10.0] - 2026-04-23
### Updated
- obf order status history change for createdDatetime
  
## [4.9.0] - 2026-04-21
### Updated
- Storefulfillment join to use releaseid
  
## [4.8.0] - 2026-04-17
### Updated
- Added where condition for dim_loc for NA 

## [4.7.0] - 2026-04-16
### Updated
- Corrected OH amounts,checkdigit,milton update,resoncode in refunds
  
## [4.6.0] - 2026-04-15
### Updated
- Cancelreasons,OH amounts,discount amounts,reject on obf

## [4.5.0] - 2026-04-10
### Updated
- Product master and OH amount changes,Gift cards

## [4.4.9] - 2026-04-09
### Updated
- obf_order_status_history filter removed and case statement in order_header

## [4.4.8] - 2026-04-07
### Updated
- obf_order_status_history pm update

## [4.4.7] - 2026-04-07
### Updated
- PM changes all across 

## [4.4.6] - 2026-04-07
### Updated
- updated consignments to use online banner for MAO
  
## [4.4.5] - 2026-04-07
### Updated
- order header fix 

## [4.4.4] - 2026-04-07
### Updated
- new order header changes 
  
## [4.4.3] - 2026-04-06
### Updated
- corrected db details

## [4.4.2] - 2026-04-06
### Added
- corrected sql statement
  
## [4.4.1] - 2026-04-06
### Added
- updated refunds and returns to align with prod

## [4.4.0] - 2026-04-06
### Added
- Added all dom related sqls and scala files for canada go-live

## [4.3.6] - 2026-03-31
### Added
- ref_source column to all the base tables

## [4.3.5] - 2025-09-15
### Updated
- Modified AdlsExchange_Gen2 code to perform weekly vacuum and optimize 

## [4.3.4] - 2025-07-29
### Updated
- Modified AdlsReturn_Gen2, AdlsRefunds_Gen2 code with new column new columns order_lineNumber

## [4.3.3] - 2025-06-24
### Updated
- Added Weekly Optimize Vacuum

## [4.3.2] - 2025-05-28
### Updated
- Returns and refunds Dl table with new column pii

## [4.3.1] - 2025-01-06
### Updated
- Returns and refunds Dl table with new column

## [4.3.0] - 2024-06-25
### Updated
- useragent new columns in oms orderline npii and pii

## [4.2.9] - 2024-06-18
### Updated
- corrected missed comma in the merge statement of AdlsOrders_Gen2

## [4.2.8] - 2024-06-18
### Updated
- Modified the code of Orders & Returns to capture the loyalty schema changes related to FLX2.0

## [4.2.7] - 2024-05-28
### Updated
- Modified the code of tender to fixe edge case and add load date partition.
- 
## [4.2.6] - 2024-02-07
### Updated
- Modified the code to run either in batch or streaming based on the input parameter.

## [4.2.5] - 2024-02-02
### Updated
- Modified the code to run either in batch or streaming based on the input parameter.

## [4.2.4] - 2023-11-02
### Updated
- Updated the products_fixed_cost path to gen2

## [4.2.3] - 2023-10-10
### Updated
- mergeSchema to returns

## [4.2.2] - 2023-10-10
### Updated
- Order of columns for the merge statement updated for Returns

## [4.2.1] - 2023-10-10
### Added
- Added columns for order(storeAddress) and returns(returningStore,xstoreTranscationNumber) to both Gen1 and Gen2 classes

## [4.2.0] - 2023-09-09
### Added
- Added AdlsOMSTender : parsing payment tag to new tender raw
### Updated
- Updated AdlsLoadMain_Gen2, common_gen2 to parse the data to AdlsOMSTender.

## [4.1.0] - 2023-08-31
### Updated
- cogs attribute derivation logic update - bug fix

## [4.0.0] - 2023-07-28
### Updated
- updated changes for Kafka SaaS Migration to both Gen1 and Gen2 classes

## [3.1.0] - 2023-06-14
### Added
- Added _Gen2 files to support gen2
- Targeted dependencies to Databricks 12.2 LTS

## [3.0.2] - 2022-09-23
### Updated
- update etl to fix nulls in cogs

## [2.6.5] - 2021-01-13
### Updated
- Hotfix for PEST-735 (changed order_Discount amount to String)

## 2.6.4 - 2021-01-13
### Added
- added payment column to order header

## 2.6.3 - 2020-10-23
### Updated
- Hotfix for powerbi refresh

##  2.6.2 - 2020-10-22
### Updated
- OBF changes
