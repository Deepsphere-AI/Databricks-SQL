# Databricks notebook source
display('select * from dsai_sales_analysis.dsai_fact')

# COMMAND ----------

display(spark.sql("select product_family_id,sum(revenue) from dsai_sales_analysis.dsai_fact group by product_family_id"));

# COMMAND ----------


