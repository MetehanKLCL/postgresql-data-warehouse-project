/*
================================================================================================================================
DDL Script: Create Gold Views
================================================================================================================================

Script Purpose: 
  - This Script creates Views for the Gold layer in the Data Warehouse
  - The Gold Layer represents the final Fact and Dimension Tables (Star Schema)

  - Each view performs transformations and combine data from Silver Layer to produce a clean enriched and business ready dataset

Script Usage:
  - These Gold Layer views are designed to be directly accessible by business users for reporting and analytics
================================================================================================================================
*/



-- Creating Dimension Customers View for Gold Layer

DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers
SELECT
	ROW_Number() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_name,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN   ci.cst_gndr -- CRM is the master database for gender info
		ELSE COALESCE (ca.gen, 'n/a')
	END gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

-- Creating Products Dimension View for Gold Layer

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, 
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt	AS start_date 
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- In order to filter out all data;

-- Creating Sales Fact View for Gold Layer
  
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_date AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;
