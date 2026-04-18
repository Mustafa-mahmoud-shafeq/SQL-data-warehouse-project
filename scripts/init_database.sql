/*
    =========================================================
    Project : Medilian Data Warehouse
    File    : init_database.sql
    Purpose : Initialize database and core schemas
              (bronze, silver, gold)
    Author  : Mustafa M. Shafeq
    Warning : Existing database or schema names matching this
              script will not be recreated.
    =========================================================
*/

USE master;
GO

IF DB_ID('Medilian_DWH') IS NULL
BEGIN
    CREATE DATABASE Medilian_DWH;
END
GO

USE Medilian_DWH;
GO

-- Bronze layer: raw ingested data
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

-- Silver layer: cleaned and transformed data
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- Gold layer: business-ready and aggregated data
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO
