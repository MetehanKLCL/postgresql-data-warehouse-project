CALL silver.load_bronze();
  
CREATE OR REPLACE PROCEDURE silver.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration INTERVAL;
BEGIN
	start_time := clock_timestamp();
	
	RAISE NOTICE '=======================';
	RAISE NOTICE 'Loading SILVER Layer...';
	RAISE NOTICE '=======================';

	-- crm_cust_info
	BEGIN
		TRUNCATE TABLE silver.crm_cust_info;

		INSERT INTO silver.crm_cust_info (
			cst_id, cst_key, cst_firstname, cst_lastname,
			cst_marital_status, cst_gndr, cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname),
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END,
			cst_create_date
		FROM (
			SELECT *,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1;

		RAISE NOTICE 'silver.crm_cust_info loaded.';
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
	END;

	-- crm_prd_info
	BEGIN
		TRUNCATE TABLE silver.crm_prd_info;

		INSERT INTO silver.crm_prd_info (
			prd_id, cat_id, prd_key, prd_nm,
			prd_cost, prd_line, prd_start_dt, prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
			SUBSTRING(prd_key, 7, LENGTH(prd_key)),
			prd_nm,
			COALESCE(prd_cost, 0),
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'R' THEN 'Road'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END,
			prd_start_dt,
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1
		FROM bronze.crm_prd_info;

		RAISE NOTICE 'silver.crm_prd_info loaded.';
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
	END;

	-- crm_sales_details
	BEGIN
		TRUNCATE TABLE silver.crm_sales_details;

		INSERT INTO silver.crm_sales_details (
			sls_ord_num, sls_prd_key, sls_cust_id,
			sls_order_date, sls_ship_dt, sls_due_dt,
			sls_sales, sls_quantity, sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
				ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
			END,
			CASE
				WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
				ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
			END,
			CASE
				WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
				ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
			END,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_sales <= 0 
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END
		FROM bronze.crm_sales_details;

		RAISE NOTICE 'silver.crm_sales_details loaded.';
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
	END;

	-- erp_cust_az12
	BEGIN
		TRUNCATE TABLE silver.erp_cust_az12;

		INSERT INTO silver.erp_cust_az12 (
			cid, bdate, gen
		)
		SELECT 
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
				ELSE cid
			END,
			CASE
				WHEN bdate > NOW() THEN NULL
				ELSE bdate
			END,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END
		FROM bronze.erp_cust_az12;

		RAISE NOTICE 'silver.erp_cust_az12 loaded.';
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error loading erp_cust_az12: %', SQLERRM;
	END;

	-- erp_loc_a101
	BEGIN
		TRUNCATE TABLE silver.erp_loc_a101;

		INSERT INTO silver.erp_loc_a101 (
			cid, cntry
		)
		SELECT 
			REPLACE(cid, '-', ''),
			CASE 
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END
		FROM bronze.erp_loc_a101;

		RAISE NOTICE 'silver.erp_loc_a101 loaded.';
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error loading erp_loc_a101: %', SQLERRM;
	END;

	-- erp_px_cat_g1v2
	BEGIN
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		INSERT INTO silver.erp_px_cat_g1v2 (
			id, cat, subcat, maintenance
		)
		SELECT 
			id, cat, subcat, maintenance
		FROM bronze.erp_px_cat_g1v2;

		RAISE NOTICE 'silver.erp_px_cat_g1v2 loaded.';
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error loading erp_px_cat_g1v2: %', SQLERRM;
	END;

	-- Total duration
	end_time := clock_timestamp();
	duration := end_time - start_time;

	RAISE NOTICE '===================================';
	RAISE NOTICE 'SILVER Layer Loaded Successfully!';
	RAISE NOTICE 'Total Duration: %', duration;
	RAISE NOTICE '===================================';
END;
$$;
