CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
        PRINT 'WE ARE LOADING THE SILVER LAYER';
        PRINT '=========================================';

        /* -------------------------------------------------
           LOADING CRM TABLES
           ------------------------------------------------- */
		SET @start_time = GETDATE();
        PRINT '-----------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '-----------------------------------------';
		-- >> INSERTING CLEANSED DATA OF TABLE: crm_cust_info INTO SILVER LAYER
		PRINT '>> TRUNCATING TABLE: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> INSERTING DATA TO TABLE: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
		cst_id,cst_key,cst_firstname,cst_lastname, cst_marital_status , cst_gndr ,cst_create_date
		)
		SELECT 
			cst_id ,
			cst_key ,
			TRIM(cst_firstname) ,
			TRIM(cst_lastname),
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status ,
			CASE WHEN UPPER(trim(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(trim(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
			Select *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info)t
			where flag_last=1;
			SET @end_time = GETDATE();
			PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
			PRINT ' >> ---------------------';

		
		-- >> INSERTING CLEANSED DATA OF TABLE: crm_prd_info INTO SILVER LAYER
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> INSERTING DATA TO TABLE: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info ([prd_info], [cat_id], [prd_key], [prd_nm], [prd_cost], [prd_line], [prd_start_dt], [prd_end_dt])
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
			CAST(DATEADD(DAY,-1,LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		FROM 
			bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT ' >> ---------------------';

		-- >> INSERT DATA INTO SILVER LAYER TABLE: crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> INSERTING DATA TO TABLE: silver.crm_sales_details'
		INSERT INTO [silver].[crm_sales_details] (
					[sls_ord_num], [sls_prd_key], [sls_cust_id],
					[sls_order_dt], [sls_ship_dt], [sls_due_dt],
					[sls_sales], [sls_quantity], [sls_price])
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE cast(cast(sls_order_dt AS varchar) AS DATE)
			END sls_order_dt ,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE cast(cast(sls_ship_dt AS varchar) AS DATE)
			END sls_ship_dt ,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE cast(cast(sls_due_dt AS varchar) AS DATE)
			END sls_due_dt ,
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
		FROM 
			bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';

		/* -------------------------------------------------
        LOADING ERP TABLES
        ------------------------------------------------- */
		
		-- >> INSERT DATA INTO SILVER LAYER TABLE: erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> INSERTING DATA TO TABLE: silver.erp_cust_az12'
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
		FROM 
			[bronze].[erp_cust_az12]
		SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';



		-- >> INSERT DATA INTO SILVER LAYER TABLE: erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> INSERTING DATA TO TABLE: silver.erp_loc_a101'
		INSERT INTO [silver].[erp_loc_a101] ([cid], [cntry])
		SELECT
			REPLACE(cid,'-','') cid,
			CASE
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('US' , 'USA' ) THEN 'United States'
				WHEN UPPER(TRIM(cntry)) = '' or cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
			END AS cntry
		FROM
			bronze.erp_loc_a101;
		PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';

		-- >> INSERT DATA INTO SILVER LAYER TABLE: erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> INSERTING DATA TO TABLE: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT 
			id, 
			cat,
			subcat,
			maintenance
		FROM 
			bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION :' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
        PRINT ' >> ---------------------';

	END TRY
	BEGIN CATCH
		PRINT '=================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================================';
	END CATCH

	SET @batch_end_time = GETDATE();
	PRINT ' >> SILVER LAYER LOAD DURATION :' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'SECONDS';
END
