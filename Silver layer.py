# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver layer script

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### data access using app

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading data

# COMMAND ----------

df_calender=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Calender")

# COMMAND ----------

df_customer=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Customers")

# COMMAND ----------

df_categories=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Categories")

# COMMAND ----------

df_products=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Products")

# COMMAND ----------

df_returns=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Returns")

# COMMAND ----------

df_sales2015=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Sakes2015")

# COMMAND ----------

df_sales2016=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Sales2016")

# COMMAND ----------

df_sales=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Sales*")

# COMMAND ----------

df_sales2017=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Sales2017")

# COMMAND ----------

df_extendedSales=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/extendedSales")

# COMMAND ----------

df_subCategories=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Sub_Categories")

# COMMAND ----------

df_territories=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adventureworkstoragedl.dfs.core.windows.net/Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

df_calender.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create new column for month and year which have month for each date 
# MAGIC ### withcolumn: either craete new column or modify existing one 

# COMMAND ----------

# MAGIC %md
# MAGIC #### calender

# COMMAND ----------

df_calender=df.withColumn('Month',month(df['Date'])).withColumn('Year',year(df['Date']))
df_calender.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### write data in silver layer
# MAGIC ### 4 modes 
# MAGIC ##### append(): merge data in existing one or apply union
# MAGIC ##### overwrite(): replace the data or store the fresh data
# MAGIC ##### error(): data alredy there and just rtying to write the data in same folder then it throw error
# MAGIC ##### ignore(): data exists then it will not the error but will not write the data as well

# COMMAND ----------

df_calender.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Calender").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### customers 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### text transformation
# MAGIC ##### create new column in which concat all 3 columns name to crete the full name (prefix, firstName, lastName) 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2 methods to concat
# MAGIC ###### 1. concat()
# MAGIC ###### 2. concat_ws()

# COMMAND ----------

df_customer.withColumn("fullName", concat(col("Prefix"), lit(' '), col("FirstName"), lit(' '), col("LastName"))).display()

# COMMAND ----------

df_customer=df_customer.withColumn("fullName", concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))
df_customer.display()

# COMMAND ----------

df_customer.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Customers").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### sub categories

# COMMAND ----------

df_subCategories.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Sub_Categories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### product

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fetch first 2 letters of product SKU , all the aphabets before hyphen
# MAGIC ##### fetch color from product name using split()
# MAGIC ##### 

# COMMAND ----------

df_products=df_products.withColumn("ProductSKU", split(col('ProductSKU'), '-')[0])\
    .withColumn("ProductName", split(col('ProductName'), ' ')[0])
df_products.display()

# COMMAND ----------

df_products.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Products").save()

# COMMAND ----------

df_returns.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Returns").save()

# COMMAND ----------

df_territories.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Territories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### sales

# COMMAND ----------

# MAGIC %md
# MAGIC ##### transform stockDate to timestamp 
# MAGIC ##### convert alphabet S with T in OrderNumber
# MAGIC ##### multiply orderLineItem with orderQuantity

# COMMAND ----------

df_sales2015.display()

# COMMAND ----------

df_sales2015=df_sales2015.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales2015=df_sales2015.withColumn('OrderNumber',regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales2015=df_sales2015.withColumn('multiply', col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_sales2015.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### aggregate
# MAGIC ##### how many orders are we received in one day

# COMMAND ----------

df_sales2015.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_categories.display()

# COMMAND ----------

df_territories.display()

# COMMAND ----------

df_sales2015.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Sakes2015").save()

# COMMAND ----------

df_sales=df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales=df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales2015=df_sales2015.withColumn('multiply', col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_sales.write.format("parquet").mode('append').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Sales").save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales=df_sales.withColumn('OrderDate',expr("date_add(OrderDate, 365*8)"))

# COMMAND ----------

df_sales.withColumn('OrderDate', expr("date_add(OrderDate, 365*8)")).display()

# COMMAND ----------

    from pyspark.sql.functions import min, max

date_range = df_sales.agg(
    min("OrderDate").alias("min_date"),
    max("OrderDate").alias("max_date")
).collect()[0]

print("Current Min:", date_range["min_date"])
print("Current Max:", date_range["max_date"])


# COMMAND ----------

from pyspark.sql.functions import expr, rand, floor, datediff, lit, date_add, col, concat, monotonically_increasing_id

# Step 1: Set range
start_date = "2015-01-01"
end_date   = "2025-04-30"

# Step 2: Expand data (so you get more rows)
N = 10  # increase this to multiply dataset size
df_expanded = df_sales.withColumn("dup_id", monotonically_increasing_id() % N)

# Step 3: Random OrderDate between 2015-01-01 and 2025-04-30
df_expanded = df_expanded.withColumn(
    "OrderDate",
    expr(f"date_add('{start_date}', cast(floor(rand() * datediff('{end_date}', '{start_date}')) as int))")
)

# Step 4: StockDate = OrderDate + random(1–15 days)
df_expanded = df_expanded.withColumn(
    "StockDate",
    expr("date_add(OrderDate, cast(floor(rand()*15) + 1 as int))")
)

# Step 5: Randomize OrderNumber (unique-ish)
df_expanded = df_expanded.withColumn(
    "OrderNumber",
    concat(col("OrderNumber"), lit("_"), floor(rand()*100000))
)

# Step 6: Randomize OrderLineItem (1–5) and OrderQuantity (1–20)
df_expanded = df_expanded.withColumn(
    "OrderLineItem", (floor(rand()*5) + 1)
).withColumn(
    "OrderQuantity", (floor(rand()*20) + 1)
)


# COMMAND ----------

df_expanded.display()

# COMMAND ----------

from pyspark.sql.functions import expr, rand, floor, datediff, lit, date_add, col, concat, monotonically_increasing_id

# Step 1: Set range
start_date = "2015-01-01"
end_date   = "2025-04-30"

# Step 2: Expand data (duplicate rows to increase size)
N = 10  # adjust as needed
df_expanded = df_sales.crossJoin(spark.range(N))  # replaces dup_id trick

# Step 3: Random OrderDate between 2015-01-01 and 2025-04-30
df_expanded = df_expanded.withColumn(
    "OrderDate",
    expr(f"date_add('{start_date}', CAST(FLOOR(rand() * datediff('{end_date}', '{start_date}')) AS INT))")
)

# Step 4: StockDate = OrderDate + random(1–15 days)
df_expanded = df_expanded.withColumn(
    "StockDate",
    expr("date_add(OrderDate, CAST(FLOOR(rand()*15 + 1) AS INT))")
)

# Step 5: Randomize OrderNumber (unique-ish)
df_expanded = df_expanded.withColumn(
    "OrderNumber",
    concat(col("OrderNumber"), lit("_"), floor(rand()*100000))
)

# Step 6: Randomize OrderLineItem (1–5) and OrderQuantity (1–20)
df_expanded = df_expanded.withColumn(
    "OrderLineItem", (floor(rand()*5) + 1)
).withColumn(
    "OrderQuantity", (floor(rand()*20) + 1)
)

# Step 7: Make CustomerKey unique for every row
df_expanded = df_expanded.withColumn(
    "CustomerKey", monotonically_increasing_id()
)


# COMMAND ----------

df_expanded.display()

# COMMAND ----------

df_sales = df_expanded

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_expanded

# COMMAND ----------

df_sales.write.format("parquet").mode('overwrite').option("path", "abfss://silver@adventureworkstoragedl.dfs.core.windows.net/Sales").save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

