-- Databricks notebook source
use dsai_sales_analysis;

-- COMMAND ----------

CREATE OR REPLACE TABLE dsai_sales_analysis.DSAI_product_family_egcp1(
Product_Family_ID string Not NULL ,
Product_Family_Name string NOT NULL
)USING DELTA
Location 'gs://dsai-sales-analysis-gcp-bucket/external-storage/product_family1.csv';

-- COMMAND ----------

COPY INTO dsai_sales_analysis.dsai_product_family_egcp1
FROM '/FileStore/dsai_sales_analysis/Product_Family.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE');

select * from dsai_product_family_egcp1;

-- COMMAND ----------

describe extended dsai_sales_analysis.dsai_product_family_egcp1;

-- COMMAND ----------


