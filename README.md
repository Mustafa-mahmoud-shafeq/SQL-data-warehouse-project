# SQL Data Warehouse Project

A modern **SQL Server Data Warehouse** project built using the **Medallion Architecture** approach (**Bronze, Silver, Gold**).  
The project demonstrates how to design a layered warehouse, ingest raw CSV data, clean and standardize it, and publish business-ready analytical views.

---

## Overview

This project builds a complete data warehouse pipeline in SQL Server from raw source files to analytical reporting objects.

It is designed to show practical skills in:

- Data warehouse design
- ETL / ELT workflow implementation in SQL
- Data cleansing and standardization
- Dimensional modeling
- Data quality validation
- Building analytics-ready views

The warehouse uses three layers:

- **Bronze** → raw data ingestion from source files
- **Silver** → cleansed and standardized data
- **Gold** → business-ready dimensional views for reporting and analytics

---

## Architecture

### 1) Bronze Layer
The Bronze layer stores raw data exactly as it comes from the source systems with minimal transformation.

Source domains used in this project:

- **CRM**
  - customer information
  - product information
  - sales details

- **ERP**
  - customer demographic data
  - customer location data
  - product category data

This layer is mainly used for:
- raw historical capture
- traceability
- source-level validation
- reprocessing if needed

---

### 2) Silver Layer
The Silver layer applies cleansing, transformation, and standardization logic to the Bronze data.

Examples of transformations implemented:

- trimming unwanted spaces
- standardizing coded values such as gender and marital status
- deduplicating customer records
- correcting invalid or missing numeric values
- converting integer-based dates into proper date format
- deriving product end dates using `LEAD()`
- normalizing country values
- preparing structured data for dimensional modeling

This layer acts as the trusted, cleaned foundation of the warehouse.

---

### 3) Gold Layer
The Gold layer exposes business-ready views modeled for reporting and analytics.

Views created in this project:

- `gold.dim_customers`
- `gold.dim_product`
- `gold.fact_sales`

These views follow a **star-schema-style** design:

- **Dimensions**
  - customers
  - products

- **Fact**
  - sales transactions

This makes the data easier to consume in BI tools such as Power BI or Tableau.

---

## Project Structure

```bash
SQL-data-warehouse-project-main/
│
├── datasets/
│   └── placeholders
│
├── docs/
│   └── placeholder
│
├── scripts/
│   ├── init_database.sql
│   │
│   ├── bronze/
│   │   ├── ddl_bronze_SQL
│   │   └── proc_load_bronze.sql
│   │
│   ├── silver/
│   │   ├── ddl_silver.sql
│   │   └── proc_load_silver.sql
│   │
│   └── gold/
│       └── gold_layer_views_creation.sql
│
├── tests/
│   ├── quality_checks_silver.sql
│   └── quality_check_glod_layer.sql
│
├── LICENSE
└── README.md
