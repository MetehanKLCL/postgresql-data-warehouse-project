/*
========================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
========================================================
Script Purpose:
This script loads data into the Bronze schema tables from external CSV files
This scripts performs the following actions:
  -Truncates the bronze layer before loading data.
  -Uses the COPY command to load data from CSV files to bronze tables
  -Calculates the execution time with helps of start and end times

*/

CALL bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
	start_time := clock_timestamp();
  RAISE NOTICE '======================';
	RAISE NOTICE 'Loading BRONZE Layer';
	RAISE NOTICE '======================';

    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM '/Users/metehankilicli/Desktop/DataWareHouse Project/datasets/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER true); 
        RAISE NOTICE 'crm_cust_info loaded.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error at loading crm_cust_info: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM '/Users/metehankilicli/Desktop/DataWareHouse Project/datasets/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER true); 
        RAISE NOTICE 'crm_prd_info loaded.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error at loading crm_prd_info: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
        FROM '/Users/metehankilicli/Desktop/DataWareHouse Project/datasets/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER true); 
        RAISE NOTICE 'crm_sales_details loaded.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error at loading crm_sales_details: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM '/Users/metehankilicli/Desktop/DataWareHouse Project/datasets/source_erp/CUST_AZ12.csv'
        WITH (FORMAT csv, HEADER true); 
        RAISE NOTICE 'erp_cust_az12 loaded.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error at loading erp_cust_az12: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM '/Users/metehankilicli/Desktop/DataWareHouse Project/datasets/source_erp/LOC_A101.csv'
        WITH (FORMAT csv, HEADER true); 
        RAISE NOTICE 'erp_loc_a101 loaded.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error at loading erp_loc_a101: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM '/Users/metehankilicli/Desktop/DataWareHouse Project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (FORMAT csv, HEADER true); 
        RAISE NOTICE 'erp_px_cat_g1v2 loaded.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error at loading erp_px_cat_g1v2: %', SQLERRM;
    END;

  RAISE NOTICE '=================================';
	RAISE NOTICE 'BRONZE Layer Loaded Successfully';
	RAISE NOTICE '=================================';
	
	end_time := clock_timestamp();
	duration := end_time - start_time;

	RAISE NOTICE 'Load Duration: %', duration;
	

END;
$$;
