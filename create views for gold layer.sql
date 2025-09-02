-----------------------------
-- create view calender
-----------------------------
CREATE VIEW gold.calender
AS 
SELECT * 
FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Calender/' , FORMAT = 'PARQUET'
) as Query1

-----------------------------
-- create view customer
-----------------------------
CREATE VIEW gold.customers AS SELECT * FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Customers/' , FORMAT = 'PARQUET'
) as Query1

-----------------------------
-- create view products
-----------------------------
CREATE VIEW gold.products AS SELECT * FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Products/' , FORMAT = 'PARQUET'
) as Query1

-----------------------------
-- create view returns
-----------------------------
CREATE VIEW gold.returnProduct AS SELECT * FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Returns/' , FORMAT = 'PARQUET'
) as Query1

-----------------------------
-- create view sales 2015
-----------------------------
CREATE VIEW gold.sales2015 AS SELECT * FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Sakes2015/' , FORMAT = 'PARQUET'
) as Query1

-----------------------------
-- create view sales
-----------------------------
CREATE VIEW gold.sales AS SELECT * FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Sales/' , FORMAT = 'PARQUET'
) as Query1

-----------------------------
-- create view Sub_Categories
-----------------------------
CREATE VIEW gold.Sub_Categories AS SELECT * FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Sub_Categories/' , FORMAT = 'PARQUET'
) as Query1

-----------------------------
-- create view Territories
-----------------------------
CREATE VIEW gold.Territories AS SELECT * FROM OPENROWSET (
    BULK 'https://adventureworkstoragedl.dfs.core.windows.net/silver/Territories/' , FORMAT = 'PARQUET'
) as Query1