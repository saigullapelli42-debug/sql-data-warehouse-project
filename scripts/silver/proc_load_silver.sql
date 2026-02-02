
/*
===============================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from Cleaned bronze data. 
    It performs the following actions:
    - In this First Clean the data from bronze tables and load into silvers tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

USE DataWarehouse
GO
create or alter PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
        begin try 

        set @batch_start_time = GETDATE();
        set @start_time = GETDATE();

        print '>> Truncating Table : silver.crm_cust_info';
        Truncate table [silver].[crm_cust_info];
        print '>> Inserting date into silver.crm_cust_info';

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
        
        set @end_time = GETDATE();

        print '=====================================';
        PRINT 'Loading silver.crm_cust_info layer is completed';
        PRINT 'Total Load Duration : ' + CAST(datediff(second,@start_time,@end_time)as NVARCHAR) + 'seconds'
        print '=====================================';
        
        set @start_time = GETDATE();

        print '>> Truncating Table : silver.crm_prd_info';
        Truncate table [silver].[crm_prd_info];
        print '>> Inserting date into silver.crm_prd_info';

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

        set @end_time = GETDATE();

        print '=====================================';
        PRINT 'Loading silver.crm_prd_info layer is completed';
        PRINT 'Total Load Duration : '+ CAST(datediff(second,@start_time,@end_time)as NVARCHAR) + 'seconds'
        print '=====================================';
        

        set @start_time = GETDATE();

        print '>> Truncating Table : silver.crm_sales_details';
        Truncate table [silver].[crm_sales_details];
        print '>> Inserting date into silver.crm_sales_details';

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
        
        set @end_time = GETDATE();


        print '=====================================';
        PRINT 'Loading silver.crm_sales_details layer is completed';
        PRINT 'Total Load Duration : '+ CAST(datediff(second,@start_time,@end_time)as NVARCHAR) + 'seconds'
        print '=====================================';
        
        
        set @start_time = GETDATE();

        print '>> Truncating Table : silver.erp_cust_az12';
        Truncate table [silver].[erp_cust_az12];
        print '>> Inserting date into silver.erp_cust_az12';

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
        
        set @end_time = GETDATE();

        print '=====================================';
        PRINT 'Loading silver.erp_cust_az12 layer is completed';
        PRINT 'Total Load Duration : '+ CAST(datediff(second,@start_time,@end_time)as NVARCHAR) + 'seconds'
        print '=====================================';
        
        set @start_time = GETDATE();

        print '>> Truncating Table : silver.erp_loc_a101';
        Truncate table [silver].[erp_loc_a101];
        print '>> Inserting date into silver.erp_loc_a101';

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
        
        set @end_time = GETDATE();

        print '=====================================';
        PRINT 'Loading silver.erp_loc_a101 layer is completed';
        PRINT 'Total Load Duration : '+ CAST(datediff(second,@start_time,@end_time)as NVARCHAR) + 'seconds'
        print '=====================================';
        
        set @start_time = GETDATE();

        print '>> Truncating Table : silver.erp_px_cat_g1v2';
        Truncate table [silver].[erp_px_cat_g1v2];
        print '>> Inserting date into silver.erp_px_cat_g1v2';

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

        set @end_time = GETDATE();

        print '=====================================';
        PRINT 'Loading silver.erp_px_cat_g1v2 layer is completed';
        PRINT 'Total Load Duration : '+ CAST(datediff(second,@start_time,@end_time)as NVARCHAR) + 'seconds'
        print '=====================================';

        set @batch_end_time = GETDATE();

        print '=====================================';
        PRINT 'Loading silver layer is completed';
        PRINT 'Total Load Duration : '+ CAST(datediff(second,@batch_start_time,@batch_end_time)as NVARCHAR) + 'seconds'
        print '=====================================';

    end try 
    BEGIN CATCH
        print '===========================';
        PRINT 'Error occured while loading';
        print 'error message :' + ERROR_MESSAGE();
        print 'error message :' + cast(ERROR_NUMBER() as NVARCHAR);
        print 'error message :' + cast(ERROR_STATE() as NVARCHAR);
    end catch

END

EXEC silver.load_silver
