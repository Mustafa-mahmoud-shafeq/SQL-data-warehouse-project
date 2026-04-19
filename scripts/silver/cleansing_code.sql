-- ===========================================================
-- DATA CLEANSING AND LOADING SCRIPT
-- Bronze Layer  -> Silver Layer
-- Purpose:
--   1. Validate data quality in bronze tables
--   2. Standardize and cleanse the data
--   3. Load the cleansed data into silver tables
-- ===========================================================


-- ===========================================================
-- CLEANING AND LOADNIG crm_cust_info
-- ===========================================================

-- Check bronze layer data quality and detect mistakes
-- Check unwanted spaces
-- Expectation: no results
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr);

-- Check distinct values for data standardization and consistency
Select DISTINCT cst_marital_status
from bronze.crm_cust_info;

Select DISTINCT cst_gndr
from bronze.crm_cust_info;

-- Check duplicates and null customer IDs
select cst_id , count(*) 
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL;


-- Truncate target silver table before reloading
PRINT '>> TRUNCATING TABLE: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;

-- Insert cleansed customer data into silver layer
PRINT '>> INSERTING DATA TO TABLE: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END cst_marital_status,
    CASE 
        WHEN UPPER(trim(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(trim(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END cst_gndr,
    cst_create_date
FROM (
    -- Keep only the latest record per customer ID
    Select *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
) t
where flag_last = 1;


-- Validate the cleansed silver customer table
-- Check unwanted spaces
-- Expectation: no results
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

select cst_gndr
from silver.crm_cust_info
where cst_gndr != TRIM(cst_gndr);

-- Check standardized values
Select DISTINCT cst_marital_status
from silver.crm_cust_info;

Select DISTINCT cst_gndr
from silver.crm_cust_info;

-- Check duplicates and null customer IDs in silver layer
select cst_id , count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL;

-- Final customer data preview
SELECT * From silver.crm_cust_info;


-- ===========================================================
-- CLEANING AND LOADNIG crm_prd_info
-- ===========================================================

-- Check duplicate product IDs
SELECT 
    prd_info, 
    count (*)
FROM bronze.crm_prd_info
GROUP BY prd_info 
Having count (*) > 1 or prd_info is NULL;

-- Check unwanted spaces in product name
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for null or negative product cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost is NULL;

-- Check product line values
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info
WHERE prd_line is NULL;

-- Check for invalid date order
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Truncate silver product table
PRINT '>> TRUNCATING TABLE: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;

-- Insert cleansed product data into silver layer
PRINT '>> INSERTING DATA TO TABLE: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
    [prd_info],
    [cat_id],
    [prd_key],
    [prd_nm],
    [prd_cost],
    [prd_line],
    [prd_start_dt],
    [prd_end_dt]
)
SELECT 
    prd_info, 
    replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
    prd_nm,
    ISNULL(prd_cost,0) as prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'ROAD'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    cast(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        DATEADD(
            DAY,
            -1,
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
        ) AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_info;
    

-- ===========================================================
-- CLEANING AND LOADNIG crm_sales_details
-- ===========================================================

-- Ensure that sales order number has no unwanted spaces
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != trim(sls_ord_num);

-- Check referential integrity for customer IDs
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN
    (SELECT DISTINCT cst_id FROM [silver].[crm_cust_info]);

-- Check order date format and validity
SELECT
    NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 or LEN(sls_order_dt) != 8;

-- Validate sls_sales = sls_quantity * sls_price
-- Also ensure all numeric values are positive
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price;

-- Preview cleansing logic for incorrect sales/price records
SELECT DISTINCT
    sls_sales AS old_sls_sales,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE
        When sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END sls_price
FROM bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price;

-- Truncate silver sales table
PRINT '>> TRUNCATING TABLE: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details

-- Insert cleansed sales details into silver layer
PRINT '>> INSERTING DATA TO TABLE: silver.crm_sales_details';
INSERT INTO [silver].[crm_sales_details] (
    [sls_ord_num],
    [sls_prd_key],
    [sls_cust_id],
    [sls_order_dt],
    [sls_ship_dt],
    [sls_due_dt],
    [sls_sales],
    [sls_quantity],
    [sls_price]
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE cast(cast(sls_order_dt AS varchar) AS DATE)
    END sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE cast(cast(sls_ship_dt AS varchar) AS DATE)
    END sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE cast(cast(sls_due_dt AS varchar) AS DATE)
    END sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END sls_sales,
    sls_quantity,
    CASE
        When sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END sls_price
FROM bronze.crm_sales_details;


-- ===========================================================
-- CLEANING AND LOADNIG erp_cust_az12
-- ===========================================================

-- Validate CID values against customer keys in silver.crm_cust_info
SELECT 
    cid,
    CASE
        WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
        ELSE cid
    END cid,
    bdate,
    gen
FROM [bronze].[erp_cust_az12]
WHERE
    CASE
        WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
        ELSE cid
    END NOT IN (SELECT DISTINCT [cst_key] FROM [silver].[crm_cust_info]);

-- Check invalid birth dates
SELECT DISTINCT
    bdate
FROM [bronze].[erp_cust_az12]
where bdate < '1924-02-01' OR bdate > GETDATE();

-- Check gender low-cardinality values
SELECT DISTINCT
    gen
FROM [bronze].[erp_cust_az12];
    
-- Truncate silver ERP customer table
PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12

-- Insert cleansed ERP customer data
PRINT '>> INSERTING DATA TO TABLE: silver.erp_cust_az12';
INSERT INTO [silver].[erp_cust_az12] ([cid], [bdate], [gen])
SELECT 
    CASE
        WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
        ELSE cid
    END cid,
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'Male'
        ELSE 'n/a'
    END gen
FROM [bronze].[erp_cust_az12];


-- ===========================================================
-- CLEANING AND LOADNIG erp_loc_a101
-- ===========================================================

-- Check CID integrity against customer keys
SELECT
    cid, 
    cntry
FROM bronze.erp_loc_a101
WHERE cid NOT IN (
    SELECT DISTINCT cst_key FROM silver.crm_cust_info
);

-- Check country values
SELECT DISTINCT
    cntry
FROM bronze.erp_loc_a101;

-- Preview country standardization solution
SELECT DISTINCT
    cntry as old_cntry,
    CASE
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('US' , 'USA' ) THEN 'United States'
        WHEN UPPER(TRIM(cntry)) = '' or cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;

-- Truncate silver ERP location table
PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101

-- Insert cleansed ERP location data
PRINT '>> INSERTING DATA TO TABLE: silver.erp_loc_a101';
INSERT INTO [silver].[erp_loc_a101] ([cid], [cntry])
SELECT
    REPLACE(cid,'-','') cid,
    CASE
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('US' , 'USA' ) THEN 'United States'
        WHEN UPPER(TRIM(cntry)) = '' or cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;


-- ===========================================================
-- CLEANING AND LOADNIG erp_px_cat_g1v2
-- ===========================================================

-- Check category IDs against silver product category IDs
SELECT 
    id, 
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT DISTINCT cat_id
    FROM silver.crm_prd_info
);

-- Check for unwanted spaces
SELECT 
    id, 
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) or subcat != TRIM(subcat)
    OR maintenance != TRIM(maintenance);

-- Check low-cardinality values
SELECT DISTINCT 
    cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
    subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
    maintenance
FROM bronze.erp_px_cat_g1v2;
    
-- Truncate silver category table
PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;

-- Insert ERP category data into silver layer
PRINT '>> INSERTING DATA TO TABLE: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT 
    id, 
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
