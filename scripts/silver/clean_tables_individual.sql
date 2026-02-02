/*
Note : Only for reference don't Excute this script 
You can go to proc_load_silver.sql
===============================================================================
Clean data Script: Each table clean data
===============================================================================
Script Purpose:
    This Only for reference what we are consider for Cleaned data results.
===============================================================================
*/

-- For Bronze.Cust_info into Sliver.Cust_info:

-- Check nulls and duplicates in primary key :
-- Expectation : No Results.

SELECT cst_id,count(*) from [bronze].[crm_cust_info]
GROUP BY cst_id HAVING count(*) >1; -- in this query we found duplicate entries.

-- Now we clean the data without duplicates.

INSERT into [silver].[crm_cust_info] (
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
trim(cst_firstname) as cst_firstname, -- Check for unwanted spaces :
trim(cst_lastname) as cst_lastname, -- Check for unwanted spaces :
case
    when upper(trim(cst_material_status)) = 'S' then 'Single'
    when upper(trim(cst_material_status)) = 'M' then 'Married'
    else 'n/a'
end cst_marital_status,
case
    when upper(trim(cst_gndr)) = 'F' then 'Female'
    when upper(trim(cst_gndr)) = 'M' then 'Male'
    else 'n/a'
end cst_gndr,
cst_create_date
 from (
SELECT *,ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as Leatest from [bronze].[crm_cust_info]
) t where Leatest = 1;

SELECT * from bronze.crm_cust_info;
SELECT* from [silver].[crm_cust_info];

----------------------------------------------------------------------------------------------------------------

-- For [bronze].[crm_prd_info] into [silver].[crm_prd_info]

use DataWarehouse;

SELECT * from [bronze].[crm_prd_info]

-- We need to check Any duplicates and null's in our primary key or not :

SELECT prd_id, COUNT(*) from [bronze].[crm_prd_info]
group by prd_id
having count(*) > 1 or prd_id is null

-- Check unwanted spaces and null's :
-- No Results Come.

SELECT prd_nm from [bronze].[crm_prd_info]
where prd_nm != trim(prd_nm) or prd_nm is null;

-- Check the Null Values and Negative Values :
-- No Results Found.

SELECT prd_cost from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- We need to found Distinct Vlaues :
-- And We need to set full name for that letters.

SELECT distinct(prd_line) from bronze.crm_prd_info;

-- Note : We Need to write case when statemants in bulk u can use the also

-- CASE UPPER(TRIM(prd_line))
--     when = 'M' then 'Mountain'
--     when = 'R' then 'Road'
--     when = 'S' then 'Other Sales'
--     when = 'T' then 'Touring'
--     ELSE 'n/a'
-- end

-- Now We need to see the Strat date and End date and logic is start_date < end_date :
-- No Results Found.

SELECT * from bronze.crm_prd_info WHERE prd_start_dt < prd_end_dt;

-- Now we need to check which column is matching with another table data for join.

-- In [bronze].[crm_prd_info] Table Having data like 'CO-RF-FR-R92B-58' 
-- In [bronze].[erp_px_cat_g1v2] Having data like 'CO_RF' only Now we need to retrive the data same as in this table.
-- And [bronze].[crm_prd_info] Table Having data like 'CO-RF-FR-R92B-58'.
-- In [bronze].[crm_sales_details] Having data like 'BK-R93R-62' only Now we need to retrive the data same as in this table.

-- cleaned data was created after that we need to load into silver.crm_prd_info

SELECT * FROM [silver].[crm_prd_info]

INSERT into [silver].[crm_prd_info] (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
case
    when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
    when UPPER(TRIM(prd_line)) = 'R' then 'Road'
    when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
    when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
    else 'n/a'
end prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as DATE) as prd_end_dt
from 
[bronze].[crm_prd_info];

SELECT * from silver.crm_prd_info;

---------------------------------------------------------------------------------------------------------------

/*
1.Clean the data from [bronze].[crm_sales_details] into [silver].[crm_sales_details]
2.We need to check All Dates format and data type in table structure
  I found in table structure the data type is in int but we need set date data type.
  In sql server not support int to date that's why first we need to set varchar after that date format.
--- CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) ---
3.Now we need to check sales,price and quantity not allow null values and negative values.
    sales = abs(price) * nullif(quantity,0)
    price = nullif(sales,0) / nullif(quantity,0)
 quantity = nullif(sales,0) / abs(price)
*/

INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;

-----------------------------------------------------------------------------------------------------------------

/*
1.Clean data from bronze.erp_cust_az12 and insert into silver.erp_cust_az12 :
2.For join column data is not matched that's why we modified data 
Note : we will not add any data into tables we will modify the data, like trim.
use DataWarehouse
select * from [bronze].[crm_cust_info] -- AW00011001
select * from [bronze].[erp_cust_az12] -- NASAW00011000
3.bdate is not in Feature.
    bdate > getdate()
4.gen we need to check F is Female, M is Male and null is 'n/a'.
*/

INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;

-----------------------------------------------------------------------------------------------------------------
/*
Clean the data from [bronze].[erp_loc_a101] and insert into [silver].[erp_loc_a101]
But first we need to check matching column for joining the tables.
I found match column in [bronze].[crm_cust_info] table (cst_key) AW00011000 and [bronze].[erp_loc_a101] cid (AW-00011000)
in the above column data one '-' is extra remaining same.
replace(cid,'-','')

*/
use DataWarehouse

INSERT into silver.erp_loc_a101(
    cid,
    cntry
)

SELECT 
REPLACE(cid,'-','') as cid,
case
    when TRIM(REPLACE(REPLACE(cntry,CHAR(13),''),CHAR(10),'')) = 'DE' then 'Germany'
    when TRIM(replace(REPLACE(cntry,CHAR(13),''),CHAR(10),'')) in ('US','USA') then 'United States'
    when TRIM(replace(REPLACE(cntry,CHAR(13),''),CHAR(10),'')) = '' or cntry is null then 'n/a'
    else TRIM(REPLACE(REPLACE(cntry,CHAR(13),''),CHAR(10),''))
end as cntry
from [bronze].[erp_loc_a101];

SELECT * from [bronze].[crm_cust_info];
SELECT * from silver.erp_loc_a101;

-----------------------------------------------------------------------------------------------------------
/*
Clean the data from [bronze].[erp_px_cat_g1v2] and insert into [silver].[erp_px_cat_g1v2]
But first we need to check matching column for joining the tables.
I found match column in [bronze].[crm_prd_info]; table (prd_key) CO-RF-FR-R92B-58 and [erp_px_cat_g1v2] id (AC_BR)

-- Checking Rules

i.  Id is matching no need to do any thing.
ii. Cat need to check any null and extra space in frount and back.
iii.subcat need to check any null and extra space in frount and back.
iv. maintenance need to check any null and extra space in frount and back.
Note : in Azure data studio we get extra space for lasr column is varchar data type.
you can use this trim(replace(replace(maintenance,char(13),''),char(10),''))

select * from [bronze].[erp_px_cat_g1v2];
SELECT * from [bronze].[crm_prd_info];
select * from [silver].[erp_px_cat_g1v2];

*/
INSERT into [silver].[erp_px_cat_g1v2](
id,
cat,
subcat,
maintenance
)

SELECT 
id,
cat,
subcat,
trim(replace(replace(maintenance,char(13),''),char(10),'')) as maintenance
from 
bronze.erp_px_cat_g1v2;

-------------------------------------------------------------------------------------------------------------------
