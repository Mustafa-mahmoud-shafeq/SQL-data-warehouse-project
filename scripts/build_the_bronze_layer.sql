/*
    =========================================================
    Project : Medilian Data Warehouse
    File    : build_the_bronze_layer.sql
    Purpose : Create bronze tables, load bronze data,
              and run validation queries
    Author  : Mustafa M. Shafeq
    Note    : Code below is arranged as provided.
    =========================================================
*/


/* =========================================================
   SECTION 1: CREATE BRONZE TABLES
   ========================================================= */

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

GO


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_prd_info (
    prd_info INT,
    prd_key NVARCHAR(50),
    prd_nm INT,
    prd_cost NVARCHAR(50),
    prd_line NVARCHAR(50),
    prd_start_dt NVARCHAR(50),
    prd_end_dt DATE
);

GO


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

GO


IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

GO


IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

GO


IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);


/* =========================================================
   SECTION 2: LOAD BRONZE LAYER PROCEDURE
   ========================================================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '=========================================';
        PRINT 'WE ARE LOADING THE BRONZE LAYER';
        PRINT '=========================================';

        /* -------------------------------------------------
           LOADING CRM TABLES
           ------------------------------------------------- */
        PRINT '-----------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '-----------------------------------------';

        SET @start_time = GETDATE();
        PRINT ' >> TRUNCATING TABLE: [bronze].[crm_cust_info]';
        TRUNCATE TABLE [bronze].[crm_cust_info];
        PRINT ' >> INSERTING DATA TO TABLE: [bronze].[crm_cust_info]';
        BULK INSERT [bronze].[crm_cust_info]
        FROM 'F:\sql-data-warehouse-project-withbaraa\datasets\source_crm\cust_info.csv'
        WITH (
            FirstROW = 2, -- skip header row
            FIELDTERMINATOR = ',', -- delimeter is a comma
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';


        SET @start_time = GETDATE();
        PRINT ' >> TRUNCATING TABLE: [bronze].[crm_prd_info]';
        TRUNCATE TABLE [bronze].[crm_prd_info];
        PRINT ' >> INSERTING DATA TO TABLE: [bronze].[crm_prd_info]';
        BULK INSERT [bronze].[crm_prd_info]
        FROM 'F:\sql-data-warehouse-project-withbaraa\datasets\source_crm\prd_info.csv'
        WITH (
            FirstROW = 2, -- skip header row
            FIELDTERMINATOR = ',', -- delimeter is a comma
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';

        SET @start_time = GETDATE();
        PRINT ' >> TRUNCATING TABLE: [bronze].[crm_sales_details]';
        TRUNCATE TABLE [bronze].[crm_sales_details];
        PRINT ' >> INSERTING DATA TO TABLE: [bronze].[crm_sales_details]';
        BULK INSERT [bronze].[crm_sales_details]
        FROM 'F:\sql-data-warehouse-project-withbaraa\datasets\source_crm\sales_details.csv'
        WITH (
            FirstROW = 2, -- skip header row
            FIELDTERMINATOR = ',', -- delimeter is a comma
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';


        /* -------------------------------------------------
           LOADING ERP TABLES
           ------------------------------------------------- */
        SET @start_time = GETDATE();
        PRINT '-----------------------------------------';
        PRINT 'LOADING ERP TABLES';
        PRINT '-----------------------------------------';
        PRINT ' >> TRUNCATING TABLE: [bronze].[erp_cust_az12]';
        TRUNCATE TABLE [bronze].[erp_cust_az12];
        PRINT ' >> INSERTING DATA TO TABLE: [bronze].[erp_cust_az12]';
        BULK INSERT [bronze].[erp_cust_az12]
        FROM 'F:\sql-data-warehouse-project-withbaraa\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';

        SET @start_time = GETDATE();
        PRINT ' >> TRUNCATING TABLE: [bronze].[erp_loc_a101]';
        TRUNCATE TABLE [bronze].[erp_loc_a101];
        PRINT ' >> INSERTING DATA TO TABLE: [bronze].[erp_loc_a101]';
        BULK INSERT [bronze].[erp_loc_a101]
        FROM "F:\sql-data-warehouse-project-withbaraa\datasets\source_erp\LOC_A101.csv"
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';

        SET @start_time = GETDATE();
        PRINT ' >> TRUNCATING TABLE: [bronze].[erp_px_cat_g1v2]';
        TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
        PRINT ' >> INSERTING DATA TO TABLE: [bronze].[erp_px_cat_g1v2]';
        BULK INSERT [bronze].[erp_px_cat_g1v2]
        FROM "F:\sql-data-warehouse-project-withbaraa\datasets\source_erp\PX_CAT_G1V2.csv"
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';

    END TRY
    BEGIN CATCH
        PRINT '=================================================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=================================================================';
    END CATCH

    SET @end_time = GETDATE();
    PRINT ' >> BRONZE LAYER LOAD DURATION :' + CAST(DATEDIFF(SECOND, @batch_start_time, @end_time) AS NVARCHAR) + 'SECONDS';
END


/* =========================================================
   SECTION 3: VALIDATION QUERIES
   ========================================================= */

SELECT *
FROM [bronze].[erp_px_cat_g1v2];

SELECT COUNT(*)
FROM [bronze].[erp_px_cat_g1v2];


SELECT *
FROM [bronze].[erp_loc_a101];

SELECT COUNT(*)
FROM [bronze].[erp_loc_a101];


SELECT *
FROM [bronze].[erp_cust_az12];

SELECT COUNT(*)
FROM [bronze].[erp_cust_az12];


SELECT *
FROM [bronze].[crm_sales_details];

SELECT COUNT(*)
FROM [bronze].[crm_sales_details];


SELECT *
FROM [bronze].[crm_prd_info];

SELECT COUNT(*)
FROM [bronze].[crm_prd_info];


SELECT *
FROM [bronze].[crm_cust_info];

SELECT COUNT(*)
FROM [bronze].[crm_cust_info];
