# Databricks notebook source
# MAGIC %md
# MAGIC # ELT
# MAGIC ## Extract from CSV using dataframe
# MAGIC ## Load to Delta using dataframe 
# MAGIC ## Transform to aggregate data
# MAGIC ## Display data in Bar Chart
# MAGIC ---------------

# COMMAND ----------

diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

# COMMAND ----------

diamonds.write.format("delta").mode("overwrite").save("/delta/diamonds")

# COMMAND ----------

from pyspark.sql.functions import avg
display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Futher create a table from Delta location created in step 2 in SQL 
# MAGIC ## Show aggregate data in SQL

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/';
# MAGIC SELECT * from diamonds;
