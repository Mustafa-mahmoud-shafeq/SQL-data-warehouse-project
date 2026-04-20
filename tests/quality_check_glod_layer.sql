-- =======================================================================
-- dim.customer CREATION VIEW CHECK
-- =======================================================================
-- >> check inrtrgration between tables and join return
-- >> check if duplication after join or not
SELECT cst_id , count(*)
FROM
	(SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM 
		silver.crm_cust_info ci
	LEFT JOIN 
		[silver].[erp_cust_az12] ca
	ON 
		ci.cst_key = ca.cid
	LEFT JOIN
		[silver].[erp_loc_a101] la
	ON
		ci.cst_key = la.cid
	WHERE
		ci.cst_firstname IS NOT NULL)t
GROUP BY cst_id
Having COUNT(*) > 1;


-- >> Check data integrity over diffrent sources

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE	
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
	END new_gen
FROM 
	silver.crm_cust_info ci
LEFT JOIN 
	[silver].[erp_cust_az12] ca
ON 
	ci.cst_key = ca.cid
LEFT JOIN
	[silver].[erp_loc_a101] la
ON
	ci.cst_key = la.cid
WHERE
	ci.cst_firstname IS NOT NULL;


-- =======================================================================
-- dim.product CREATION VIEW CHECK
-- =======================================================================
-- >> CHECK IF THERE IS ANY DUPLICATION AFTER JOINING

SELECT
	prd_key , count(*)
FROM
	(SELECT
		pn.prd_info, 
		pn.cat_id, 
		pn.prd_key, 
		pn.prd_nm, 
		pn.prd_cost, 
		pn.prd_line, 
		pn.prd_start_dt, 
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM
		silver.crm_prd_info pn
	LEFT JOIN
		[silver].[erp_px_cat_g1v2] pc
	ON 
		pn.cat_id = pc.id
	where 
		prd_end_dt is NULL) t  -- FILTER OUT ALL HISTORICAL DATA
GROUP BY
	prd_key
having count(*) > 1 ;

-- =======================================================================
-- fact.sales CREATION VIEW CHECK
-- =======================================================================
-- FORIGHN KEY INTEGRITY (DIMENSIONS)

SELECT *
FROM [gold].[fact_sales] sls
LEFT JOIN [gold].[dim_product] pr
ON sls.[product_key] = pr.[product_key]
LEFT JOIN [gold].[dim_customers] cu
ON sls.[customer_key] = cu.[customer_key];
