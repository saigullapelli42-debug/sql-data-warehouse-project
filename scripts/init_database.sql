/*
============================
Create Database and Schemas
============================

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists. If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
Running this script will drop the entire 'DataWarehouse' database if it exists.

All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/

use master;
GO

-- Drop and Recreate the DataWarehouse Database

IF exists(select 1 from sys.databases where name = 'DataWarehouse')

BEGIN
ALTER DATABASE DataWarehouse set single_user with ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;

GO

-- Create the 'DataWarehouse' Database

create database DataWarehouse;
GO
use DataWarehouse;
GO
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO
