/*
================================================================
DDL Script: Create Silver Tables
================================================================

Script Purpose: The script creates tables in Silver schema
Run this script to re-define the DDL structure of Silver "Tables"
================================================================
*/


CREATE TABLE IF NOT EXISTS silver.crm_cust_info
(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR (50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.crm_prd_info
(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.crm_sales_details
(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_date INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_loc_a101
(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS silver.erp_cust_az12
(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2
(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
