
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE or ALTER procedure bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
    begin TRY
    set @batch_start_time = GETDATE();
        print '====================';
        print 'Loading Bronze Layer';
        print '====================';

        -- For Bulk Data insert we using below Query :
        set @start_time = GETDATE();

        print '--------------------------';
        print 'Truncate the CRM_CUST_INFO';
        print '--------------------------';

        TRUNCATE table [bronze].[crm_cust_info];

        print '----------------------------------';
        print 'Bulk Insert Into the CRM_CUST_INFO';
        print '----------------------------------';

        BULK INSERT [bronze].[crm_cust_info]
        FROM '/var/opt/mssql/cust_info.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2, -- Skip header
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n'
        );
        
        set @end_time = GETDATE();

        PRINT '--------------------------';
        PRINT 'Duration for Execution : ' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        PRINT '--------------------------';

        print '-------------------------';
        print 'Truncate the CRM_PRD_INFO';
        print '-------------------------';

        TRUNCATE TABLE [bronze].[crm_prd_info]

        set @start_time = GETDATE();

        print '----------------------------------';
        print 'Bulk Insert Into the CRM_PRD_INFO';
        print '----------------------------------';

        BULK INSERT [bronze].[crm_prd_info]
        FROM '/var/opt/mssql/prd_info.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2, -- Skip header
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n'
        );
        
        set @end_time = GETDATE();

        PRINT '--------------------------';
        PRINT 'Duration for Execution : ' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        PRINT '--------------------------';


        print '-------------------------';
        print 'Truncate the CRM_PRD_INFO';
        print '-------------------------';

        TRUNCATE TABLE [bronze].[crm_sales_details]

        set @start_time = GETDATE();

        print '----------------------------------';
        print 'Bulk Insert Into the CRM_SALES_DETAILS';
        print '----------------------------------';

        BULK INSERT [bronze].[crm_sales_details]
        FROM '/var/opt/mssql/sales_details.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2, -- Skip header
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n'
        );

        set @end_time = GETDATE();

        PRINT '--------------------------';
        PRINT 'Duration for Execution : ' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        PRINT '--------------------------';

        print '--------------------------';
        print 'Truncate the ERP_CUST_AZ12';
        print '--------------------------';

        TRUNCATE TABLE [bronze].[erp_cust_az12]

        set @start_time = GETDATE();

        print '----------------------------------';
        print 'Bulk Insert Into the ERP_CUST_AZ12';
        print '----------------------------------';

        BULK INSERT [bronze].[erp_cust_az12]
        FROM '/var/opt/mssql/cust_az12.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2, -- Skip header
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n'
        );

        set @end_time = GETDATE();

        PRINT '--------------------------';
        PRINT 'Duration for Execution : ' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        PRINT '--------------------------';


        print '-------------------------';
        print 'Truncate the ERP_LOC_A101';
        print '-------------------------';

        TRUNCATE TABLE [bronze].[erp_loc_a101]

        set @start_time = GETDATE();

        print '---------------------------------';
        print 'Bulk Insert Into the ERP_LOC_A101';
        print '---------------------------------';

        BULK INSERT [bronze].[erp_loc_a101]
        FROM '/var/opt/mssql/loc_a101.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2, -- Skip header
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n'
        );

        set @end_time = GETDATE();

        PRINT '--------------------------';
        PRINT 'Duration for Execution : ' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        PRINT '--------------------------';

        print '----------------------------';
        print 'Truncate the ERP_PX_CAT_G1V2';
        print '----------------------------';

        TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]

        set @start_time = GETDATE();

        print '------------------------------------';
        print 'Bulk Insert Into the ERP_PX_CAT_G1V2';
        print '------------------------------------';

        BULK INSERT [bronze].[erp_px_cat_g1v2]
        FROM '/var/opt/mssql/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2, -- Skip header
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            KEEPNULLS -- This ensures empty columns in the file stay NULL in the database
        );

        set @end_time = GETDATE();

        PRINT '--------------------------';
        PRINT 'Duration for Execution : ' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        PRINT '--------------------------';
    set @batch_end_time = GETDATE()
    print '------------------------------';
    print 'Total Duration For Execution :' + cast(datediff(second,@batch_start_time,@batch_end_time) as NVARCHAR) + 'seconds';
    print '------------------------------';
    END TRY
    BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END
