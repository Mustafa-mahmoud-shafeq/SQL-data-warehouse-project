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
