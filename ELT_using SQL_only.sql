-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### ELT in SQL 
-- MAGIC - 1.Create table using csv
-- MAGIC - 2.Run aggregate query in SQL

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds
USING csv
OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")


-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color
