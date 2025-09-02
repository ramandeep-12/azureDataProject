CREATE  MASTER KEY ENCRYPTION BY PASSWORD = 'Password@123'

-- create db credentials

CREATE DATABASE SCOPED CREDENTIAL cred_raman
WITH IDENTITY='Managed Identity'

CREATE DATABASE SCOPED CREDENTIAL silver_sales_cred
WITH IDENTITY='Managed Identity'

-- create external data source (silver data source)
-- create 2 silver(read data) and gold(push data)

CREATE EXTERNAL DATA SOURCE silver_source WITH (
    LOCATION = 'https://adventureworkstoragedl.blob.core.windows.net/silver',
    CREDENTIAL = cred_raman
)

CREATE EXTERNAL DATA SOURCE silver_sales_source WITH (
    LOCATION = 'https://adventureworkstoragedl.blob.core.windows.net/silver',
    CREDENTIAL = silver_sales_cred
)

CREATE EXTERNAL DATA SOURCE gold_source WITH (
    LOCATION = 'https://adventureworkstoragedl.blob.core.windows.net/gold',
    CREDENTIAL = cred_raman
)

-- External file format

CREATE EXTERNAL FILE FORMAT parquet_format WITH(
    FORMAT_TYPE= PARQUET,
    DATA_COMPRESSION= 'org.apache.hadoop.io.compress.SnappyCodec'
)



-- craete external table as external sales
CREATE EXTERNAL TABLE gold.extsales WITH(
    LOCATION = 'extsales',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet_format
) AS SELECT * FROM gold.sales2015

CREATE EXTERNAL TABLE gold.extCompletesales WITH(
    LOCATION = 'extTotalSales',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet_format
) AS SELECT * FROM gold.sales

CREATE EXTERNAL TABLE gold.combinedsales WITH(
    LOCATION = 'exttSales',
    DATA_SOURCE = silver_sales_source,
    FILE_FORMAT = parquet_format
) AS SELECT * FROM gold.sales

SELECT * FROM gold.extCompletesales

SELECT * FROM gold.combinedsales

SELECT * FROM gold.extsales