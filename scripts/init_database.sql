/*

==============================
CREATING DATABASE AND SCHEMAS 
==============================

SCRIPT PURPOSE:
  This script creates a new database called DataWarehouse
  Additionally, the scripts creates 3 Schemas named: 'bronze', 'silver', 'gold'

WARNING:
  Running this script will automatically, drop the 'DataWarehouse' database, with all the data within it
  Be sure you have backup before running this script
*/

-- Drop the Database if it exists and recreate 'DataWarehouse' database
IF EXISTS 
  (
    SELECT *
    FROM  sys.databases
    WHERE name = 'DataWarehouse'
  )

  BEGIN
    ALTER DATABASE DataWarehouse SET Single_user WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END


-- Creating a Database named DataWarehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Creating Schemas to keep things organized 
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
