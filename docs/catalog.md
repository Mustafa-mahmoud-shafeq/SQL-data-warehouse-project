# Technical Project Catalog

## SQL Data Warehouse Project

This document provides a structured technical catalog for the **SQL Data Warehouse Project**. It explains the repository layout, the role of each script, and the detailed design of the **Bronze**, **Silver**, and **Gold** layers.

The project follows a layered warehouse pattern in SQL Server to move data from raw source ingestion into cleaned warehouse tables and then into business-ready analytical views.

---

## Table of Contents

1. [Project Summary](#project-summary)
2. [Architecture Overview](#architecture-overview)
3. [Repository Catalog](#repository-catalog)
4. [Execution Flow](#execution-flow)
5. [Database Initialization Layer](#database-initialization-layer)
6. [Bronze Layer Catalog](#bronze-layer-catalog)
7. [Silver Layer Catalog](#silver-layer-catalog)
8. [Gold Layer Catalog](#gold-layer-catalog)
9. [Data Quality and Testing Catalog](#data-quality-and-testing-catalog)
10. [End-to-End Data Flow](#end-to-end-data-flow)
11. [Business Output Catalog](#business-output-catalog)
12. [Current Technical Gaps and Improvement Opportunities](#current-technical-gaps-and-improvement-opportunities)

---

## Project Summary

This project builds a warehouse in **SQL Server** using a medallion-style structure:

- **Bronze** stores raw data from source files.
- **Silver** stores cleaned and standardized data.
- **Gold** publishes analytics-ready dimensional views.

The project demonstrates practical warehouse development skills such as:

- database and schema setup
- raw data ingestion with `BULK INSERT`
- SQL-based cleansing and standardization
- deduplication and data correction
- dimensional modeling with fact and dimension views
- validation through data quality checks

---

## Architecture Overview

The warehouse is divided into three logical layers.

### Bronze Layer
The Bronze layer is the landing area for raw source data. It captures the incoming data with minimum transformation so that source values remain available for auditing and reprocessing.

### Silver Layer
The Silver layer applies cleansing and business standardization rules. This is the trusted integration layer where data quality issues are corrected and source records are made consistent.

### Gold Layer
The Gold layer exposes the final reporting entities. In this project, Gold contains dimensional views that support analytical reporting and BI consumption.

---

## Repository Catalog

```text
SQL-data-warehouse-project-main/
├── datasets/
│   └── placeholders
├── docs/
│   ├── placeholder
│   └── technical_project_catalog.md
├── scripts/
│   ├── init_database.sql
│   ├── stakeholders
│   ├── bronze/
│   │   ├── ddl_bronze_SQL
│   │   └── proc_load_bronze.sql
│   ├── silver/
│   │   ├── ddl_silver.sql
│   │   └── proc_load_silver.sql
│   └── gold/
│       └── gold_layer_views_creation.sql
├── tests/
│   ├── quality_checks_silver.sql
│   ├── quality_check_glod_layer.sql
│   └── stakeholders
├── LICENSE
└── README.md
```

### Folder Purpose Summary

#### `datasets/`
Reserved for source CSV files that feed the Bronze layer.

#### `docs/`
Contains supporting documentation for the project. This catalog belongs here to explain the technical design and warehouse logic.

#### `scripts/`
Contains the SQL implementation of the warehouse:

- database setup
- Bronze layer DDL and load procedure
- Silver layer DDL and load procedure
- Gold layer analytical view creation

#### `tests/`
Contains SQL validation scripts used to check Silver and Gold layer quality.

---

## Execution Flow

The project is designed to run in the following order:

1. `scripts/init_database.sql`
2. `scripts/bronze/ddl_bronze_SQL`
3. `scripts/bronze/proc_load_bronze.sql`
4. `EXEC bronze.load_bronze`
5. `scripts/silver/ddl_silver.sql`
6. `scripts/silver/proc_load_silver.sql`
7. `EXEC silver.load_silver`
8. `scripts/gold/gold_layer_views_creation.sql`
9. `tests/quality_checks_silver.sql`
10. `tests/quality_check_glod_layer.sql`

This flow ensures that the warehouse is initialized, raw data is loaded, cleansed data is prepared, and final analytical views are created and validated.

---

## Database Initialization Layer

### Script: `scripts/init_database.sql`

This script prepares the SQL Server environment for the warehouse.

### Responsibilities

- creates the database `Medilian_DWH` if it does not already exist
- creates the three schemas:
  - `bronze`
  - `silver`
  - `gold`

### Why it matters

This script is the foundation of the project. Without it, the warehouse schemas and objects cannot be created in an organized way.

---

## Bronze Layer Catalog

The Bronze layer is the **raw ingestion layer** of the warehouse.

### Bronze Layer Goals

- store source data as received
- provide traceability to the source systems
- serve as the reload point for downstream transformations
- separate raw ingestion from business cleansing logic

### Bronze Source Domains

The Bronze layer receives data from two business domains:

#### CRM source domain
- customer information
- product information
- sales transactions

#### ERP source domain
- customer master attributes
- location data
- product category data

---

### Script: `scripts/bronze/ddl_bronze_SQL`

This script creates the raw Bronze tables.

### Bronze Tables

#### 1. `bronze.crm_cust_info`
Stores raw CRM customer data.

**Columns**
- `cst_id`
- `cst_key`
- `cst_firstname`
- `cst_lastname`
- `cst_marital_status`
- `cst_gndr`
- `cst_create_date`

**Role**
Captures raw customer master data from the CRM source before any deduplication or standardization is applied.

---

#### 2. `bronze.crm_prd_info`
Stores raw CRM product data.

**Columns**
- `prd_info`
- `prd_key`
- `prd_nm`
- `prd_cost`
- `prd_line`
- `prd_start_dt`
- `prd_end_dt`

**Role**
Captures raw product records including encoded product keys, cost, product line, and historical start and end dates.

---

#### 3. `bronze.crm_sales_details`
Stores raw CRM sales transaction data.

**Columns**
- `sls_ord_num`
- `sls_prd_key`
- `sls_cust_id`
- `sls_order_dt`
- `sls_ship_dt`
- `sls_due_dt`
- `sls_sales`
- `sls_quantity`
- `sls_price`

**Role**
Captures raw order-level sales transactions before date conversion and numeric corrections.

---

#### 4. `bronze.erp_cust_az12`
Stores ERP customer master and demographic data.

**Columns**
- `cid`
- `bdate`
- `gen`

**Role**
Provides ERP-based customer attributes that later enrich the Gold customer dimension.

---

#### 5. `bronze.erp_loc_a101`
Stores ERP location data.

**Columns**
- `cid`
- `cntry`

**Role**
Provides customer country information that later feeds the Gold customer dimension.

---

#### 6. `bronze.erp_px_cat_g1v2`
Stores ERP product category reference data.

**Columns**
- `id`
- `cat`
- `subcat`
- `maintenance`

**Role**
Provides product category mappings used later to enrich the product dimension.

---

### Script: `scripts/bronze/proc_load_bronze.sql`

This script creates the stored procedure `bronze.load_bronze`.

### Procedure Purpose

- truncates Bronze tables before each reload
- loads CSV files into Bronze using `BULK INSERT`
- prints load progress and load duration
- catches and prints SQL Server errors during execution

### Bronze Load Steps

#### CRM load sequence
1. truncate `bronze.crm_cust_info`
2. load `cust_info.csv`
3. truncate `bronze.crm_prd_info`
4. load `prd_info.csv`
5. truncate `bronze.crm_sales_details`
6. load `sales_details.csv`

#### ERP load sequence
1. truncate `bronze.erp_cust_az12`
2. load `CUST_AZ12.csv`
3. truncate `bronze.erp_loc_a101`
4. load `LOC_A101.csv`
5. truncate `bronze.erp_px_cat_g1v2`
6. load `PX_CAT_G1V2.csv`

### Bronze Validation Queries

At the end of the script, the author included validation queries that:

- preview data in Bronze tables
- count rows loaded into each Bronze table

### Bronze Layer Design Notes

The Bronze layer keeps data close to the source structure. This is useful because it preserves the original values before business corrections are applied in Silver.

---

## Silver Layer Catalog

The Silver layer is the **cleansing, standardization, and integration preparation layer**.

### Silver Layer Goals

- clean source-level issues
- standardize inconsistent values
- fix formatting problems
- convert raw values into valid data types
- prepare data for analytical consumption in Gold

### Script: `scripts/silver/ddl_silver.sql`

This script creates the Silver tables.

### Silver Tables

All Silver tables include a warehouse technical timestamp column:
- `dwh_create_date DATETIME2 DEFAULT GETDATE()`

This allows the warehouse to record when rows were loaded into the Silver layer.

---

#### 1. `silver.crm_cust_info`
Cleaned CRM customer table.

**Columns**
- `cst_id`
- `cst_key`
- `cst_firstname`
- `cst_lastname`
- `cst_marital_status`
- `cst_gndr`
- `cst_create_date`
- `dwh_create_date`

**Role**
Stores the trusted customer version after trimming, standardization, and latest-record selection.

---

#### 2. `silver.crm_prd_info`
Cleaned CRM product table.

**Columns**
- `prd_info`
- `cat_id`
- `prd_key`
- `prd_nm`
- `prd_cost`
- `prd_line`
- `prd_start_dt`
- `prd_end_dt`
- `dwh_create_date`

**Role**
Stores cleaned product information and prepares current-vs-historical product logic for Gold.

---

#### 3. `silver.crm_sales_details`
Cleaned CRM sales table.

**Columns**
- `sls_ord_num`
- `sls_prd_key`
- `sls_cust_id`
- `sls_order_dt`
- `sls_ship_dt`
- `sls_due_dt`
- `sls_sales`
- `sls_quantity`
- `sls_price`
- `dwh_create_date`

**Role**
Stores corrected sales transactions with valid date values and corrected measures.

---

#### 4. `silver.erp_cust_az12`
Standardized ERP customer table.

**Columns**
- `cid`
- `bdate`
- `gen`
- `dwh_create_date`

**Role**
Provides a cleaned ERP customer reference used to enrich customer attributes in Gold.

---

#### 5. `silver.erp_loc_a101`
Standardized ERP location table.

**Columns**
- `cid`
- `cntry`
- `dwh_create_date`

**Role**
Provides a normalized customer-country mapping used in Gold.

---

#### 6. `silver.erp_px_cat_g1v2`
Standardized ERP product category table.

**Columns**
- `id`
- `cat`
- `subcat`
- `maintenance`
- `dwh_create_date`

**Role**
Acts as the trusted category lookup for Gold product enrichment.

---

### Script: `scripts/silver/proc_load_silver.sql`

This script creates the stored procedure `silver.load_silver`.

### Procedure Purpose

- truncates Silver tables before each reload
- reads data from Bronze tables
- applies cleansing and standardization rules
- writes cleaned rows into the Silver layer
- prints load durations and catches runtime errors

### Detailed Silver Transformations

#### 1. CRM Customer Transformations

**Source:** `bronze.crm_cust_info`  
**Target:** `silver.crm_cust_info`

**Transformation rules**
- trims first and last names
- standardizes marital status:
  - `S` → `Single`
  - `M` → `Married`
  - other values → `n/a`
- standardizes gender:
  - `F` → `Female`
  - `M` → `Male`
  - other values → `n/a`
- keeps only the latest record per `cst_id` using `ROW_NUMBER()` ordered by `cst_create_date DESC`

**Business value**
This logic ensures that each customer appears once in the cleaned layer and that coded CRM values are made business-readable.

---

#### 2. CRM Product Transformations

**Source:** `bronze.crm_prd_info`  
**Target:** `silver.crm_prd_info`

**Transformation rules**
- derives `cat_id` from the first part of `prd_key`
- removes the category prefix from `prd_key` to create a cleaner product number
- replaces null product cost with `0`
- standardizes product line values:
  - `M` → `Mountain`
  - `R` → `ROAD`
  - `S` → `Other Sales`
  - `T` → `Touring`
  - other values → `n/a`
- converts `prd_start_dt` into a proper `DATE`
- derives `prd_end_dt` using `LEAD(prd_start_dt)` minus one day to build historical date ranges

**Business value**
This logic converts raw product records into a warehouse-ready structure that supports category enrichment and historical product filtering.

---

#### 3. CRM Sales Transformations

**Source:** `bronze.crm_sales_details`  
**Target:** `silver.crm_sales_details`

**Transformation rules**
- converts integer-style dates into SQL `DATE`
- converts invalid dates such as `0` or non-8-digit values to `NULL`
- recalculates `sls_sales` when:
  - sales is null
  - sales is non-positive
  - sales does not equal `quantity * ABS(price)`
- recalculates `sls_price` when:
  - price is null
  - price is non-positive
  - fallback uses `sls_sales / NULLIF(sls_quantity,0)`

**Business value**
This logic repairs common sales-data quality issues and makes transaction values reliable enough for reporting.

---

#### 4. ERP Customer Transformations

**Source:** `bronze.erp_cust_az12`  
**Target:** `silver.erp_cust_az12`

**Transformation rules**
- removes `NAS` prefix from `cid` when present
- sets future birthdates to `NULL`
- standardizes gender values:
  - `F` or `FEMALE` → `Female`
  - `M` or `MALE` → `Male`
  - other values → `n/a`

**Business value**
This makes ERP customer reference data compatible with CRM keys and suitable for customer enrichment.

---

#### 5. ERP Location Transformations

**Source:** `bronze.erp_loc_a101`  
**Target:** `silver.erp_loc_a101`

**Transformation rules**
- removes dashes from `cid`
- standardizes country values:
  - `DE` → `Germany`
  - `US` or `USA` → `United States`
  - blank or null → `n/a`
  - all other values are trimmed

**Business value**
This creates a cleaner country dimension input for Gold customer reporting.

---

#### 6. ERP Product Category Transformations

**Source:** `bronze.erp_px_cat_g1v2`  
**Target:** `silver.erp_px_cat_g1v2`

**Transformation rules**
- direct load from Bronze into Silver
- preserves category, subcategory, and maintenance values

**Business value**
This table serves as the trusted lookup for product categorization in Gold.

---

## Gold Layer Catalog

The Gold layer is the **business presentation layer** of the warehouse.

### Gold Layer Goals

- expose easy-to-consume reporting objects
- combine related cleaned datasets into dimensional views
- support BI dashboards and analysis use cases

### Script: `scripts/gold/gold_layer_views_creation.sql`

This script creates the final Gold views.

---

### 1. `gold.dim_customers`

This view creates the customer dimension.

**Sources used**
- `silver.crm_cust_info`
- `silver.erp_cust_az12`
- `silver.erp_loc_a101`

**Logic**
- generates `customer_key` using `ROW_NUMBER()`
- uses CRM data as the main customer record source
- enriches customer attributes with ERP birthdate and country
- chooses the customer gender using this priority:
  - use CRM gender if it is not `n/a`
  - otherwise use ERP gender
  - otherwise default to `n/a`
- filters out records where `cst_firstname IS NULL`

**Output columns**
- `customer_key`
- `customer_id`
- `customer_number`
- `first_name`
- `last_name`
- `country`
- `marital_status`
- `gender`
- `birthdate`
- `create_date`

**Business role**
This view provides a unified customer dimension for reporting and allows sales to be analyzed by customer attributes.

---

### 2. `gold.dim_product`

This view creates the product dimension.

**Sources used**
- `silver.crm_prd_info`
- `silver.erp_px_cat_g1v2`

**Logic**
- generates `product_key` using `ROW_NUMBER()` ordered by start date and product key
- joins Silver product data with the ERP category lookup
- keeps only current products by filtering `prd_end_dt IS NULL`

**Output columns**
- `product_key`
- `product_id`
- `product_number`
- `product_name`
- `category_id`
- `category`
- `subcategory`
- `maintenance`
- `cost`
- `product_line`
- `start_date`

**Business role**
This view provides a clean analytical product catalog with category enrichment and only active product records.

---

### 3. `gold.fact_sales`

This view creates the sales fact view.

**Sources used**
- `silver.crm_sales_details`
- `gold.dim_product`
- `gold.dim_customers`

**Logic**
- uses Silver sales as the transactional source
- links transactions to the Gold product dimension using product number
- links transactions to the Gold customer dimension using customer id

**Output columns**
- `order_number`
- `product_key`
- `customer_key`
- `order_date`
- `shipping_date`
- `due_date`
- `sales_amount`
- `quantity`
- `price`

**Business role**
This is the central measurable object of the warehouse. It allows analytical reporting by customer, product, and transaction timing.

---

## Data Quality and Testing Catalog

The project includes SQL-based validation scripts in the `tests` folder.

### Script: `tests/quality_checks_silver.sql`

This script checks the quality of Silver-layer data and also demonstrates the cleansing logic.

### Main validation themes

#### Customer checks
- unwanted spaces in names and gender
- distinct source values for marital status and gender
- duplicate or null customer IDs
- post-load verification in Silver

#### Product checks
- duplicate or null product IDs
- unwanted spaces in product names
- null or negative product cost
- missing product line values
- invalid date ordering between product start and end dates

#### Sales checks
- unwanted spaces in order number
- referential mismatch between sales customers and Silver customer records
- invalid order date values
- inconsistent sales calculations compared to quantity and price

### Why it matters

These checks help verify that Bronze issues are identified before or during Silver transformation and that the cleaned layer is suitable for Gold consumption.

---

### Script: `tests/quality_check_glod_layer.sql`

This script validates the Gold views.

### Main validation themes

#### Customer dimension checks
- verifies whether joining Silver customer, ERP customer, and ERP location creates duplicates
- validates the gender-priority logic used across sources

#### Product dimension checks
- verifies whether the category join introduces duplicated active products

#### Fact sales checks
- validates dimensional joins between the fact view and the customer and product dimensions

### Why it matters

Gold is the layer consumed by BI tools and reports. Any duplication or join mismatch here directly affects final analytics.

---

## End-to-End Data Flow

The data moves through the warehouse in the following pattern:

1. CSV source files are loaded into Bronze tables.
2. Bronze preserves raw values and file-level structure.
3. Silver reads from Bronze and applies cleaning, standardization, deduplication, and data repair logic.
4. Gold reads from Silver and builds dimensional reporting views.
5. Test scripts validate the Silver and Gold outputs.

### Source-to-Gold Mapping Summary

| Source Area | Bronze Table | Silver Table | Gold Output |
|---|---|---|---|
| CRM customers | `bronze.crm_cust_info` | `silver.crm_cust_info` | `gold.dim_customers` |
| CRM products | `bronze.crm_prd_info` | `silver.crm_prd_info` | `gold.dim_product` |
| CRM sales | `bronze.crm_sales_details` | `silver.crm_sales_details` | `gold.fact_sales` |
| ERP customer master | `bronze.erp_cust_az12` | `silver.erp_cust_az12` | `gold.dim_customers` |
| ERP location | `bronze.erp_loc_a101` | `silver.erp_loc_a101` | `gold.dim_customers` |
| ERP product categories | `bronze.erp_px_cat_g1v2` | `silver.erp_px_cat_g1v2` | `gold.dim_product` |

---

## Business Output Catalog

The Gold layer supports core analytics use cases such as:

- sales analysis by customer
- sales analysis by product
- customer demographic and country analysis
- product category and subcategory analysis
- current product catalog reporting
- order lifecycle analysis using order, ship, and due dates

This makes the project suitable for dashboards in tools such as Power BI or Tableau.

---

## Current Technical Gaps and Improvement Opportunities

While the project is structurally strong for learning and portfolio use, a few technical issues are currently visible in the scripts.

### Notable issues

- some `DROP TABLE` statements in the Bronze DDL script drop the wrong table name
- some `BULK INSERT` paths use hardcoded local Windows paths
- two `BULK INSERT` file paths use double quotes instead of single quotes
- the project currently uses views in Gold instead of persisted dimension and fact tables
- naming includes a few spelling inconsistencies such as `glod`, `LOADNIG`, and `tabels`

### Recommended improvements

- parameterize file locations instead of hardcoding them
- add ETL audit logging tables
- add row-count logging per load step
- improve exception handling and logging strategy
- convert Gold views into physical dimension and fact tables if needed for scale
- add surrogate key persistence strategy
- add incremental load support
- add an architecture diagram and ERD into the `docs` folder

---

## Final Note

This catalog is intended to make the repository easier to understand for:

- hiring managers reviewing the project as a portfolio item
- data engineering learners studying warehouse layering
- collaborators who need a quick understanding of the codebase
- future maintainers extending the warehouse

